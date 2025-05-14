import type { ChildProcess } from 'node:child_process'
import type { Server } from 'node:net'
import type { Readable } from 'node:stream'

import type { API, CameraController, CameraRecordingConfiguration, CameraRecordingDelegate, HAP, HDSProtocolSpecificErrorReason, RecordingPacket } from 'homebridge'

import type { VideoConfig } from './settings.js'
import type { Logger } from './logger.js'
import type { Mp4Session } from './settings.js'

import { Buffer } from 'node:buffer'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { createServer } from 'node:net'
import { env } from 'node:process'

import { APIEvent, AudioRecordingCodecType, H264Level, H264Profile } from 'homebridge'

import { PreBuffer } from './prebuffer.js'

import { MP4Atom, FFMpegFragmentedMP4Session, PREBUFFER_LENGTH, ffmpegPathString } from './settings.js'

/**
 * Attempts to listen on a random port and returns the port number.
 * Will retry with different ports if the initial attempt fails.
 */
export async function listenServer(server: Server, log: Logger): Promise<number> {
  let isListening = false
  let attempt = 0
  const maxAttempts = 5

  while (!isListening && attempt < maxAttempts) {
    attempt++
    const port = 10000 + Math.round(Math.random() * 30000)
    log.debug(`Attempting to listen on port ${port} (attempt ${attempt}/${maxAttempts})`)
    
    server.listen(port)
    try {
      await once(server, 'listening')
      log.debug(`Successfully listening on port ${port}`)
      isListening = true
      const address = server.address()
      if (address && typeof address === 'object' && 'port' in address) {
        return address.port
      }
      throw new Error('Failed to get server address')
    } catch (e: any) {
      log.error(`Error while listening on port ${port}:`, e)
      // Close the server before trying again
      try {
        server.close()
      } catch (closeError) {
        // Ignore errors when closing
      }
    }
  }
  
  if (!isListening) {
    log.error(`Failed to listen on any port after ${maxAttempts} attempts`)
  }
  
  // Default return if all attempts fail
  return 0
}

/**
 * Reads a specified length of data from a readable stream with timeout protection.
 * Will resolve with empty buffer if timeout occurs or stream ends.
 */
export async function readLength(readable: Readable, length: number): Promise<Buffer> {
  if (!length) {
    return Buffer.alloc(0)
  }

  // First try immediate read
  const directRead = readable.read(length)
  if (directRead) {
    return directRead
  }

  return new Promise((resolve) => {
    // Add a timeout to prevent hanging forever waiting for data
    const timeout = setTimeout(() => {
      cleanup()
      resolve(Buffer.alloc(0))
    }, 15000) // 15 second timeout (increased from 10s)

    // Read handler
    const handleReadable = (): void => {
      const data = readable.read(length)
      if (data) {
        cleanup()
        resolve(data)
      }
    }

    // End/error handler
    const handleEnd = (): void => {
      cleanup()
      // Return empty buffer instead of rejecting to avoid crashes
      resolve(Buffer.alloc(0))
    }

    // Cleanup function to remove all listeners
    const cleanup = (): void => {
      clearTimeout(timeout)
      readable.removeListener('readable', handleReadable)
      readable.removeListener('end', handleEnd)
      readable.removeListener('error', handleEnd)
      readable.removeListener('close', handleEnd)
    }

    // Set up listeners
    readable.on('readable', handleReadable)
    readable.on('end', handleEnd)
    readable.on('error', handleEnd)
    readable.on('close', handleEnd)
  })
}

/**
 * Parse fragmented MP4 data from a readable stream.
 * Includes robust error handling and logging.
 */
export async function* parseFragmentedMP4(readable: Readable, log?: Logger, cameraName?: string): AsyncGenerator<MP4Atom> {
  const logMsg = log 
    ? (msg: string) => log.debug(msg, cameraName)
    : (msg: string) => console.log(msg)

  try {
    let boxCount = 0
    
    while (true) {
      try {
        // Read header (8 bytes)
        const header = await readLength(readable, 8)
        
        // Check if header is empty (stream ended)
        if (!header || header.length === 0 || header.length < 8) {
          logMsg('Stream ended or returned empty data during MP4 header parsing')
          return
        }
        
        // Parse length and type from header
        const length = header.readInt32BE(0) - 8
        const type = header.slice(4).toString()
        
        // Validate length to avoid invalid memory allocation
        if (length < 0 || length > 100 * 1024 * 1024) { // 100MB max to prevent excessive allocation
          logMsg(`Invalid MP4 box length: ${length} bytes for type ${type}`)
          return
        }
        
        // Read the data portion
        const data = await readLength(readable, length)
        
        // Check if data is complete
        if (!data || data.length === 0 || (length > 0 && data.length < length)) {
          logMsg(`Stream ended while reading MP4 box data for type ${type}`)
          return
        }
        
        boxCount++
        
        // Only log every 5th box or important ones to reduce log spam
        if (boxCount % 5 === 0 || type === 'moov' || type === 'mdat' || type === 'ftyp') {
          logMsg(`Processing MP4 box: type=${type}, length=${length}`)
        }
        
        yield {
          header,
          length,
          type,
          data,
        }
      } catch (boxError) {
        logMsg(`Error parsing MP4 box: ${boxError}`)
        return // End generator instead of throwing
      }
    }
  } catch (streamError) {
    logMsg(`Fatal error in MP4 parsing: ${streamError}`)
    return
  }
}

export class RecordingDelegate implements CameraRecordingDelegate {
  // Private class members
  private readonly hap: HAP
  private readonly log: Logger
  private readonly cameraName: string
  private readonly videoConfig?: VideoConfig
  private process?: ChildProcess
  private readonly videoProcessor: string
  readonly controller?: CameraController
  
  // Session tracking
  private preBufferSession?: Mp4Session
  private preBuffer?: PreBuffer
  private activeRecordingSessions: Map<number, {
    cp?: ChildProcess;
    startTime?: number;
    timeout?: NodeJS.Timeout;
    subscriberId?: number;
    fragments?: Buffer[];
    isComplete?: boolean;
  }> = new Map()
  
  // State tracking
  private recordingConfiguration?: CameraRecordingConfiguration
  private isRecordingActive = false
  private streamCloseHandlers = new Map<number, () => void>()
  
  // Statistics for diagnostics
  private recordingStats = {
    totalSessions: 0,
    successfulSessions: 0,
    errorSessions: 0,
    averageDuration: 0,
    totalFragmentsSent: 0
  }

  constructor(log: Logger, cameraName: string, videoConfig: VideoConfig, api: API, hap: HAP, videoProcessor?: string) {
    this.log = log
    this.hap = hap
    this.cameraName = cameraName
    this.videoProcessor = videoProcessor || ffmpegPathString || 'ffmpeg'
    this.videoConfig = videoConfig

    // Clean up resources on Homebridge shutdown
    api.on(APIEvent.SHUTDOWN, () => {
      this.log.info('Homebridge is shutting down, cleaning up resources', this.cameraName)
      this.cleanupAllResources()
    })
    
    // Create a diagnostic report every hour to monitor for memory leaks
    setInterval(() => {
      this.logDiagnostics()
    }, 60 * 60 * 1000) // Every hour
  }
  
  /**
   * Log diagnostic information about recording sessions
   */
  private logDiagnostics(): void {
    this.log.info(`DIAGNOSTIC REPORT - Camera: ${this.cameraName}`, this.cameraName)
    this.log.info(`Active recording sessions: ${this.activeRecordingSessions.size}`, this.cameraName)
    this.log.info(`Stream close handlers: ${this.streamCloseHandlers.size}`, this.cameraName)
    this.log.info(`Recording active: ${this.isRecordingActive}`, this.cameraName)
    this.log.info(`Total sessions: ${this.recordingStats.totalSessions}`, this.cameraName)
    this.log.info(`Successful sessions: ${this.recordingStats.successfulSessions}`, this.cameraName)
    this.log.info(`Error sessions: ${this.recordingStats.errorSessions}`, this.cameraName)
    this.log.info(`Average duration: ${this.recordingStats.averageDuration.toFixed(2)}s`, this.cameraName)
    this.log.info(`Total fragments sent: ${this.recordingStats.totalFragmentsSent}`, this.cameraName)
    
    // Check for potential memory leaks
    if (this.activeRecordingSessions.size > 3) {
      this.log.warn('Potential memory leak: Too many active recording sessions', this.cameraName)
      this.cleanupStaleRecordingSessions()
    }
    
    if (this.streamCloseHandlers.size > 3) {
      this.log.warn('Potential memory leak: Too many stream close handlers', this.cameraName)
      this.streamCloseHandlers.clear()
    }
  }

  /**
   * Clean up any recording sessions that have been active for too long
   */
  private cleanupStaleRecordingSessions(): void {
    const now = Date.now()
    const maxAge = 10 * 60 * 1000 // 10 minutes
    
    for (const [streamId, session] of this.activeRecordingSessions.entries()) {
      if (session.startTime && (now - session.startTime) > maxAge) {
        this.log.warn(`Cleaning up stale recording session ${streamId} (age: ${((now - session.startTime) / 1000).toFixed(0)}s)`, this.cameraName)
        this.closeRecordingStream(streamId, undefined)
      }
    }
  }

  /**
   * Clean up all resources
   */
  private cleanupAllResources(): void {
    // Clean up all recording sessions
    for (const [streamId, session] of this.activeRecordingSessions.entries()) {
      try {
        if (session.cp) {
          session.cp.kill('SIGKILL')
        }
        if (session.timeout) {
          clearTimeout(session.timeout)
        }
      } catch (e) {
        // Ignore errors when killing processes
      }
    }
    this.activeRecordingSessions.clear()
    
    // Clean up prebuffer
    if (this.preBufferSession) {
      if (this.preBufferSession.process) {
        try {
          this.preBufferSession.process.kill('SIGKILL')
        } catch (e) {
          // Ignore errors when killing process
        }
      }
      if (this.preBufferSession.server) {
        try {
          this.preBufferSession.server.close()
        } catch (e) {
          // Ignore errors when closing server
        }
      }
      this.preBufferSession = undefined
    }
    
    // Clear all stream close handlers
    this.streamCloseHandlers.clear()
    
    this.log.info('All resources cleaned up', this.cameraName)
  }

  /**
   * Called by HomeKit when recording should be activated/deactivated
   */
  async updateRecordingActive(active: boolean): Promise<void> {
    this.log.info(`Recording active status changed to: ${active}`, this.cameraName)
    
    if (active) {
      // Start or ensure prebuffer is running when recording is active
      this.log.debug('Recording active, ensuring prebuffer is started', this.cameraName)
      await this.startPreBuffer()
      
      // Initialize other recording resources if needed
      this.isRecordingActive = true
    } else {
      // Recording is no longer active
      this.isRecordingActive = false
      
      // Clean up recording resources, except if prebuffer should continue running
      if (this.activeRecordingSessions.size === 0 && !this.videoConfig?.prebuffer) {
        // If there are no active sessions and prebuffer is not configured to continue running,
        // we can clean up resources
        if (this.preBufferSession) {
          this.log.debug('Stopping prebuffer session as recording is inactive', this.cameraName)
          if (this.preBufferSession.process) {
            this.preBufferSession.process.kill()
          }
          if (this.preBufferSession.server) {
            this.preBufferSession.server.close()
          }
          this.preBufferSession = undefined
        }
      } else if (this.videoConfig?.prebuffer) {
        this.log.debug('Recording is inactive but keeping prebuffer running as configured', this.cameraName)
      }
    }
    
    return Promise.resolve()
  }

  /**
   * Called by HomeKit when recording configuration is updated
   */
  async updateRecordingConfiguration(configuration: CameraRecordingConfiguration): Promise<void> {
    this.log.info('Recording configuration updated', this.cameraName)
    
    // Store configuration for use in other functions
    this.recordingConfiguration = configuration
    
    if (configuration) {
      this.log.debug(`Audio codec: ${configuration.audioCodec.type}, ` +
        `Video resolution: ${configuration.videoCodec.resolution[0]}x${configuration.videoCodec.resolution[1]} @ ${configuration.videoCodec.resolution[2]}fps`, 
        this.cameraName)
      
      // If recording is active, we may need to restart the prebuffer with the new settings
      if (this.isRecordingActive && this.videoConfig?.prebuffer) {
        // Restart the prebuffer with the new settings, if necessary
        await this.restartPreBuffer()
      }
    }
    
    return Promise.resolve()
  }

  /**
   * Main handler for recording stream requests from HomeKit
   */
  async *handleRecordingStreamRequest(streamId: number): AsyncGenerator<RecordingPacket, any, any> {
    this.log.info(`Recording stream request received for stream ID: ${streamId}`, this.cameraName)
    this.recordingStats.totalSessions++
    
    // Track session start time for diagnostics
    const sessionStartTime = Date.now()

    if (!this.videoConfig) {
      this.log.error('Video configuration is missing', this.cameraName)
      this.recordingStats.errorSessions++
      return
    }

    // If there's an existing recording session for this ID, clean it up first
    if (this.activeRecordingSessions.has(streamId)) {
      this.log.warn(`Found stale recording session for stream ID: ${streamId}, cleaning up`, this.cameraName)
      await this.cleanupRecordingSession(streamId)
    }

    // Flag to track if this stream has been closed
    let isStreamClosed = false

    // Setup a listener to detect when this stream is closed
    const closeHandler = () => {
      isStreamClosed = true
      this.log.debug(`Stream close detected for stream ID: ${streamId}`, this.cameraName)
      
      // Remove this handler from our tracking map
      this.streamCloseHandlers.delete(streamId)
      
      // Clean up any active recording session for this stream
      this.cleanupRecordingSession(streamId)
    }

    // Add this handler to our tracking map
    this.streamCloseHandlers.set(streamId, closeHandler)

    // Set up a watchdog timer to force stream closure after a timeout
    const streamTimeout = setTimeout(() => {
      if (!isStreamClosed) {
        this.log.warn(`Stream ${streamId} watchdog timeout triggered, forcing cleanup`, this.cameraName)
        closeHandler()
      }
    }, 120000) // 2 minute watchdog

    try {
      // Start prebuffer if configured
      await this.startPreBuffer()

      if (!this.preBuffer) {
        this.log.error('Failed to start prebuffer', this.cameraName)
        this.recordingStats.errorSessions++
        return
      }

      // Get the recording configuration
      const recordingConfiguration = this.recordingConfiguration
      if (!recordingConfiguration) {
        this.log.error('Recording configuration not available', this.cameraName)
        this.recordingStats.errorSessions++
        return
      }

      // Create a buffer to accumulate fragments
      const session = {
        fragments: [] as Buffer[],
        startTime: sessionStartTime,
        timeout: streamTimeout,
        isComplete: false
      }
      
      // Register this stream as an active recording session
      this.activeRecordingSessions.set(streamId, session)

      // This function will be called whenever we receive a new fragment from the prebuffer
      const handleFragment = (data: Buffer) => {
        if (!isStreamClosed && session.isComplete === false && session.fragments) {
          session.fragments.push(data)
        }
      }
      
      // Subscribe to the prebuffer to receive fragments directly
      const subscriberId = this.preBuffer.subscribeToPreBuffer(handleFragment)
      
      // Update the session with the subscriber ID
      this.activeRecordingSessions.set(streamId, {
        ...session,
        subscriberId
      })
      
      // Wait for the initial fragments to arrive (prebuffer should send them immediately)
      const waitForInitialFragments = new Promise<void>((resolve) => {
        // Check every 100ms if we have fragments
        const checkInterval = setInterval(() => {
          if (isStreamClosed || session.isComplete === true) {
            clearInterval(checkInterval)
            resolve()
          } else if (session.fragments.length > 0) {
            clearInterval(checkInterval)
            resolve()
          }
        }, 100)
        
        // Timeout after 5 seconds
        setTimeout(() => {
          clearInterval(checkInterval)
          resolve()
        }, 5000)
      })
      
      await waitForInitialFragments
      this.log.debug(`Initial fragments available for stream ${streamId}, fragments in queue: ${session.fragments.length}`, this.cameraName)
      
      // Process each fragment and send it to HomeKit
      let fragmentCount = 0
      let waitCycles = 0
      
      while (!isStreamClosed && session.isComplete === false) {
        // If we have no fragments, wait a bit and continue
        if (!session.fragments || session.fragments.length === 0) {
          // Prevent infinite loop if we're not receiving any fragments
          waitCycles++
          if (waitCycles > 100) { // 10 seconds with no fragments
            this.log.warn(`No fragments received for 10 seconds, ending stream ${streamId}`, this.cameraName)
            break
          }
          await new Promise(resolve => setTimeout(resolve, 100))
          continue
        }
        
        // Reset wait counter since we got fragments
        waitCycles = 0
        
        // Process available fragments
        while (session.fragments && session.fragments.length > 0 && !isStreamClosed && session.isComplete === false) {
          const fragment = session.fragments.shift()
          if (fragment) {
            fragmentCount++
            
            // Log fragment progress occasionally
            if (fragmentCount % 5 === 0) {
              this.log.debug(`Sent ${fragmentCount} fragments for stream ${streamId}, queue size: ${session.fragments.length}`, this.cameraName)
            }
            
            const packet: RecordingPacket = {
              data: fragment,
              isLast: false
            }
            
            yield packet
          }
        }
      }

      // Send final packet
      yield {
        data: Buffer.alloc(0),
        isLast: true
      }
      
      // Mark session as complete to stop fragment collection
      session.isComplete = true

      // Update statistics
      const sessionDuration = (Date.now() - sessionStartTime) / 1000 // in seconds
      this.recordingStats.successfulSessions++
      this.recordingStats.totalFragmentsSent += fragmentCount
      
      // Update average duration with a weighted calculation
      this.recordingStats.averageDuration = 
        (this.recordingStats.averageDuration * (this.recordingStats.successfulSessions - 1) + sessionDuration) / 
        this.recordingStats.successfulSessions

      this.log.info(`Recording stream ${streamId} completed successfully. Duration: ${sessionDuration.toFixed(1)}s, Fragments: ${fragmentCount}`, this.cameraName)

    } catch (error) {
      this.log.error(`Error handling recording stream: ${error}`, this.cameraName)
      this.recordingStats.errorSessions++

      // Send final packet in case of error
      yield {
        data: Buffer.alloc(0),
        isLast: true
      }
    } finally {
      // Clean up all resources
      await this.cleanupRecordingSession(streamId)
      
      // Remove our stream close handler
      this.streamCloseHandlers.delete(streamId)
    }
  }

  /**
   * Clean up a recording session
   */
  private async cleanupRecordingSession(streamId: number): Promise<void> {
    const session = this.activeRecordingSessions.get(streamId)
    if (session) {
      // Mark session as complete to stop fragment collection
      if ('isComplete' in session) {
        session.isComplete = true
      }
      
      // Cancel the watchdog timeout
      if (session.timeout) {
        clearTimeout(session.timeout)
      }
      
      // Kill the FFmpeg process if there is one
      if (session.cp) {
        try {
          session.cp.kill('SIGKILL')
        } catch (e) {
          // Ignore errors when killing process
        }
      }
      
      // Unsubscribe from prebuffer
      if ('subscriberId' in session && session.subscriberId !== undefined && this.preBuffer) {
        try {
          this.preBuffer.unsubscribeFromPreBuffer(session.subscriberId)
        } catch (e) {
          this.log.error(`Error unsubscribing from prebuffer: ${e}`, this.cameraName)
        }
      }
      
      // Clear any buffered fragments to free memory
      if ('fragments' in session) {
        session.fragments = []
      }
      
      // Remove from active sessions
      this.activeRecordingSessions.delete(streamId)
      this.log.debug(`Cleaned up recording session for stream ID: ${streamId}`, this.cameraName)
    }
  }

  /**
   * Called by HomeKit when a recording stream should be closed
   */
  closeRecordingStream(streamId: number, reason: HDSProtocolSpecificErrorReason | undefined): void {
    this.log.info(`Recording stream closed for stream ID: ${streamId}, reason: ${reason ?? 'unknown'}`, this.cameraName)

    try {
      // Signal that this stream has been closed to any active generators
      if (this.streamCloseHandlers.has(streamId)) {
        const closeHandler = this.streamCloseHandlers.get(streamId)
        if (closeHandler) {
          closeHandler()
          this.log.debug(`Triggered close handler for stream ID: ${streamId}`, this.cameraName)
        }
      }

      // Force immediate cleanup of the recording session
      this.cleanupRecordingSession(streamId)
      
      // Additional cleanup to stop any lingering processes
      if (this.process) {
        try {
          this.process.kill('SIGKILL')
          this.log.debug(`Forcefully terminated FFmpeg process for stream ${streamId}`, this.cameraName)
          this.process = undefined
        } catch (e) {
          // Ignore kill errors
        }
      }
    } catch (error) {
      this.log.error(`Error closing recording stream: ${error}`, this.cameraName)
    }
  }

  /**
   * Start or restart the prebuffer
   */
  async startPreBuffer(): Promise<void> {
    this.log.info(`Start prebuffer for ${this.cameraName}, prebuffer enabled: ${this.videoConfig?.prebuffer}`)
    if (this.videoConfig?.prebuffer) {
      // Check if prebuffer already exists and is running
      if (!this.preBuffer) {
        this.log.debug('Creating new PreBuffer instance', this.cameraName)
        this.preBuffer = new PreBuffer(this.log, this.videoConfig.source ?? '', this.cameraName, this.videoProcessor)
        
        if (!this.preBufferSession) {
          this.log.debug('Starting prebuffer session', this.cameraName)
          try {
            this.preBufferSession = await this.preBuffer.startPreBuffer()
            this.log.info('Prebuffer session started successfully', this.cameraName)
          } catch (error) {
            this.log.error(`Failed to start prebuffer: ${error}`, this.cameraName)
          }
        } else {
          this.log.debug('Prebuffer session already exists, reusing it', this.cameraName)
        }
      } else {
        this.log.debug('PreBuffer instance already exists, reusing it', this.cameraName)
        
        // Check if we need to restart the session (e.g., if it died)
        if (!this.preBufferSession || 
            (this.preBufferSession.process && this.preBufferSession.process.exitCode !== null) || 
            (this.preBufferSession.server && !this.preBufferSession.server.listening)) {
          this.log.debug('Prebuffer session needs restart', this.cameraName)
          try {
            this.preBufferSession = await this.preBuffer.startPreBuffer()
            this.log.info('Prebuffer session restarted successfully', this.cameraName)
          } catch (error) {
            this.log.error(`Failed to restart prebuffer: ${error}`, this.cameraName)
          }
        }
      }
    } else {
      this.log.debug('Prebuffer is disabled in configuration', this.cameraName)
    }
  }

  /**
   * Restart the prebuffer (used when settings change)
   */
  private async restartPreBuffer(): Promise<void> {
    this.log.debug('Restarting prebuffer with new configuration', this.cameraName)
    
    // First stop any existing prebuffer session
    if (this.preBufferSession) {
      if (this.preBufferSession.process) {
        this.preBufferSession.process.kill()
      }
      if (this.preBufferSession.server) {
        this.preBufferSession.server.close()
      }
      this.preBufferSession = undefined
    }
    
    // Reset the prebuffer instance
    if (this.preBuffer) {
      this.preBuffer = undefined // This will force the creation of a new PreBuffer object
    }
    
    // Start a new prebuffer
    await this.startPreBuffer()
  }

  /**
   * Handle generating video fragments for recordings
   */
  async * handleFragmentsRequests(configuration: CameraRecordingConfiguration): AsyncGenerator<Buffer, void, unknown> {
    this.log.debug('Video fragments requested', this.cameraName)

    const iframeIntervalSeconds = 4

    // Audio encoding parameters
    const audioArgs: Array<string> = [
      '-acodec',
      'libfdk_aac',
      ...(configuration.audioCodec.type === AudioRecordingCodecType.AAC_LC
        ? ['-profile:a', 'aac_low']
        : ['-profile:a', 'aac_eld']),
      '-ar',
      `${configuration.audioCodec.samplerate}k`,
      '-b:a',
      `${configuration.audioCodec.bitrate}k`,
      '-ac',
      `${configuration.audioCodec.audioChannels}`,
    ]

    // Video encoding parameters
    const profile = configuration.videoCodec.parameters.profile === H264Profile.HIGH
      ? 'high'
      : configuration.videoCodec.parameters.profile === H264Profile.MAIN ? 'main' : 'baseline'

    const level = configuration.videoCodec.parameters.level === H264Level.LEVEL4_0
      ? '4.0'
      : configuration.videoCodec.parameters.level === H264Level.LEVEL3_2 ? '3.2' : '3.1'

    const videoArgs: Array<string> = [
      // Comment out '-an' to enable audio in the stream
      // '-an',
      '-sn',
      '-dn',
      '-codec:v',
      'libx264',
      '-pix_fmt',
      'yuv420p',
      '-profile:v',
      profile,
      '-level:v',
      level,
      '-b:v',
      `${configuration.videoCodec.parameters.bitRate}k`,
      '-force_key_frames',
      `expr:eq(t,n_forced*${iframeIntervalSeconds})`,
      '-r',
      configuration.videoCodec.resolution[2].toString(),
    ]

    // Prepare FFmpeg input
    const ffmpegInput: Array<string> = []
    const usingPrebuffer = this.videoConfig?.prebuffer && this.preBuffer !== undefined
    const prebufferLength = configuration.mediaContainerConfiguration.fragmentLength ?? PREBUFFER_LENGTH

    if (usingPrebuffer && this.preBuffer) {
      this.log.info(`Using prebuffer for recording with length: ${prebufferLength}ms`, this.cameraName)
      try {
        const input: Array<string> = await this.preBuffer.getVideo(prebufferLength)
        if (input.length > 0) {
          ffmpegInput.push(...input)
          this.log.debug('Successfully obtained prebuffer input for recording', this.cameraName)
        } else {
          this.log.warn('Prebuffer returned empty input, falling back to direct source', this.cameraName)
          ffmpegInput.push(...(this.videoConfig?.source ?? '').split(' '))
        }
      } catch (error) {
        this.log.error(`Failed to get prebuffer video: ${error}`, this.cameraName)
        // Fallback to direct source if prebuffer fails
        ffmpegInput.push(...(this.videoConfig?.source ?? '').split(' '))
      }
    } else {
      this.log.debug('Not using prebuffer for recording, using direct source', this.cameraName)
      ffmpegInput.push(...(this.videoConfig?.source ?? '').split(' '))
    }

    this.log.debug('Starting recording process...', this.cameraName)

    let session: FFMpegFragmentedMP4Session | undefined
    
    try {
      session = await this.startFFMpegFragmentedMP4Session(this.videoProcessor, ffmpegInput, audioArgs, videoArgs)
      this.log.info('Recording process started successfully', this.cameraName)
    } catch (error) {
      this.log.error(`Failed to start recording session: ${error}`, this.cameraName)
      return
    }

    const { socket, cp, generator } = session
    let pending: Array<Buffer> = []
    let fragmentCount = 0
    let firstFragmentSent = false
    let isGeneratorActive = true

    // Track when we last received a moov or mdat box to detect stalls
    let lastBoxTime = Date.now()
    const boxTimeoutMs = 30000 // 30 seconds timeout
    const boxTimeoutInterval = setInterval(() => {
      if (!isGeneratorActive) {
        clearInterval(boxTimeoutInterval)
        return
      }
      
      const now = Date.now()
      if (now - lastBoxTime > boxTimeoutMs) {
        this.log.warn(`No moov/mdat boxes received for ${(now - lastBoxTime) / 1000}s, may indicate stalled stream`, this.cameraName)
      }
    }, 10000) // Check every 10 seconds
    
    try {
      for await (const box of generator) {
        const { header, type, length, data } = box

        pending.push(header, data)

        if (type === 'moov' || type === 'mdat') {
          lastBoxTime = Date.now() // Update timestamp on important box types
          
          const fragment = Buffer.concat(pending)
          pending = []
          fragmentCount++
          
          if (!firstFragmentSent && usingPrebuffer) {
            this.log.info('First fragment from prebuffer being sent', this.cameraName)
            firstFragmentSent = true
          }
          
          yield fragment
        }
        
        // Only log every 5th box or important ones to reduce log spam
        if (fragmentCount % 5 === 0 || type === 'moov' || type === 'ftyp') {
          this.log.debug(`MP4 box type ${type} and length: ${length}, fragment count: ${fragmentCount}`, this.cameraName)
        }
      }
    } catch (e) {
      if (e instanceof Error) {
        this.log.info(`Recording completed or encountered an error: ${e.message}`, this.cameraName)
        
        // If no fragments were sent but we have pending data, try to send what we have
        if (fragmentCount === 0 && pending.length > 0) {
          this.log.warn('No fragments sent yet, trying to send pending data', this.cameraName)
          const lastResortFragment = Buffer.concat(pending)
          yield lastResortFragment
        }
      } else {
        this.log.info(`Recording completed or encountered an unknown error`, this.cameraName)
      }
    } finally {
      isGeneratorActive = false
      this.log.info(`Recording session ended. Total fragments sent: ${fragmentCount}`, this.cameraName)
      clearInterval(boxTimeoutInterval)
      
      // Clean up resources
      try {
        if (socket) {
          socket.removeAllListeners()
          socket.destroy()
        }
        
        if (cp) {
          cp.kill('SIGKILL')
        }
      } catch (cleanupError) {
        this.log.error(`Error during cleanup: ${cleanupError}`, this.cameraName)
      }
    }
  }

  /**
   * Start an FFmpeg process for fragmented MP4 recording
   */
  async startFFMpegFragmentedMP4Session(
    ffmpegPath: string, 
    ffmpegInput: Array<string>, 
    audioOutputArgs: Array<string>, 
    videoOutputArgs: Array<string>
  ): Promise<FFMpegFragmentedMP4Session> {
    return new Promise((resolve, reject) => {
      // Create connection timeout to prevent hanging if FFmpeg doesn't connect
      const connectionTimeout = setTimeout(() => {
        reject(new Error('Timeout waiting for FFmpeg to connect to socket server'))
      }, 30000) // 30 second timeout
      
      const server = createServer((socket) => {
        clearTimeout(connectionTimeout) // Clear timeout when connected
        server.close()
        
        this.log.debug('FFmpeg connected to socket server', this.cameraName)
        
        // Handle socket events
        socket.on('error', (err) => {
          this.log.debug(`Socket error: ${err}`, this.cameraName)
        })

        socket.on('end', () => {
          this.log.debug('Socket ended by remote peer', this.cameraName)
        })

        socket.on('close', (hadError) => {
          this.log.debug(`Socket closed ${hadError ? 'with' : 'without'} error`, this.cameraName)
        })

        // Use the parseFragmentedMP4 generator with logging
        const generator = parseFragmentedMP4(socket, this.log, this.cameraName)
        
        resolve({
          socket,
          cp: this.process!,
          generator,
        })
      })

      // Try to start the server and run FFmpeg
      listenServer(server, this.log).then((serverPort) => {
        if (!serverPort) {
          reject(new Error('Failed to start FFmpeg server'))
          clearTimeout(connectionTimeout)
          return
        }
        
        const args: Array<string> = []

        // Add input arguments
        args.push(...ffmpegInput)

        // Enable audio by not using -an flag, but comment out audioOutputArgs
        // as they sometimes cause issues
        // args.push(...audioOutputArgs)

        // Add output format and video arguments
        args.push('-f', 'mp4')
        args.push(...videoOutputArgs)
        args.push('-fflags', '+genpts', '-reset_timestamps', '1')
        args.push(
          '-movflags',
          'frag_keyframe+empty_moov+default_base_moof',
          `tcp://127.0.0.1:${serverPort}`,
        )

        this.log.debug(`${ffmpegPath} ${args.join(' ')}`, this.cameraName)

        // Enable debug output from FFmpeg
        const debug = true

        const stdioValue = debug ? 'pipe' : 'ignore'
        const ffmpegProcess = spawn(ffmpegPath, args, { env, stdio: stdioValue })
        this.process = ffmpegProcess
        
        // Set up exit handling for the FFmpeg process
        ffmpegProcess.on('exit', (code, signal) => {
          this.log.debug(`FFmpeg process exited with code ${code} and signal ${signal}`, this.cameraName)
        })

        // Handle stdio (if debug enabled)
        if (debug) {
          if (ffmpegProcess.stdout) {
            ffmpegProcess.stdout.on('data', (data: Buffer) => this.log.debug(data.toString(), this.cameraName))
          }
          if (ffmpegProcess.stderr) {
            ffmpegProcess.stderr.on('data', (data: Buffer) => this.log.debug(data.toString(), this.cameraName))
          }
        }
        
        // Handle FFmpeg process errors
        ffmpegProcess.on('error', (err) => {
          this.log.error(`FFmpeg process error: ${err}`, this.cameraName)
          reject(err)
          clearTimeout(connectionTimeout)
        })
      }).catch(err => {
        this.log.error(`Failed to start FFmpeg server: ${err}`, this.cameraName)
        reject(err)
        clearTimeout(connectionTimeout)
      })
    })
  }
}