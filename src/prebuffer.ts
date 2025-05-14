import type { Server } from 'node:net'

import type { Logger } from './logger.js'
import type { MP4Atom } from './settings.js'

import { Buffer } from 'node:buffer'
import { spawn } from 'node:child_process'
import EventEmitter from 'node:events'
import { createServer } from 'node:net'
import { env } from 'node:process'

import { listenServer, parseFragmentedMP4 } from './recordingDelegate.js'
import { PrebufferFmp4, Mp4Session, defaultPrebufferDuration } from './settings.js'

export let prebufferSession: Mp4Session

export class PreBuffer {
  prebufferFmp4: Array<PrebufferFmp4> = []
  events = new EventEmitter()
  released = false
  ftyp!: MP4Atom
  moov!: MP4Atom
  idrInterval = 0
  prevIdr = 0
  private activeAtomListeners = 0
  
  // Track recording subscribers to share the prebuffer
  private recordingSubscribers: Map<number, (data: Buffer) => void> = new Map()
  
  // ID counter for subscribers
  private nextSubscriberId = 1
  
  // Flag to indicate if we have complete MP4 initialization
  private isInitialized = false

  private readonly log: Logger
  private readonly ffmpegInput: string
  private readonly cameraName: string
  private readonly ffmpegPath: string

  constructor(log: Logger, ffmpegInput: string, cameraName: string, videoProcessor: string) {
    this.log = log
    this.ffmpegInput = ffmpegInput
    this.cameraName = cameraName
    this.ffmpegPath = videoProcessor

    // Set a higher limit for the event emitter to prevent warnings
    this.events.setMaxListeners(20)
  }

  async startPreBuffer(): Promise<Mp4Session> {
    if (prebufferSession) {
      this.log.debug('Reusing existing prebuffer session', this.cameraName)
      return prebufferSession
    }
    
    this.log.debug('Starting new prebuffer', this.cameraName)
    
    const vcodec = [
      '-vcodec',
      'copy',
    ]

    // Create a TCP server that will receive the MP4 output from FFmpeg
    const fmp4OutputServer: Server = createServer(async (socket) => {
      this.log.debug('Prebuffer FFmpeg connected to server', this.cameraName)
      fmp4OutputServer.close()
      
      // Set up error handling
      socket.on('error', (err) => {
        this.log.debug(`Prebuffer socket error: ${err}`, this.cameraName)
      })
      
      // Parse incoming MP4 fragments
      const parser = parseFragmentedMP4(socket, this.log, this.cameraName)
      
      try {
        for await (const atom of parser) {
          const now = Date.now()
          
          // Store MP4 initialization boxes
          if (!this.ftyp) {
            this.ftyp = atom
            this.log.debug('Received ftyp atom in prebuffer', this.cameraName)
          } else if (!this.moov) {
            this.moov = atom
            this.log.debug('Received moov atom in prebuffer', this.cameraName)
            this.isInitialized = true
          } else {
            // Calculate IDR frame intervals for optimization
            if (atom.type === 'mdat') {
              if (this.prevIdr) {
                this.idrInterval = now - this.prevIdr
              }
              this.prevIdr = now
            }

            // Store the fragment with timestamp
            this.prebufferFmp4.push({
              atom,
              time: now,
            })
            
            // Notify all subscribers that new data is available
            this.notifySubscribers(atom)
          }

          // Maintain the circular buffer by removing old fragments
          while (this.prebufferFmp4.length && this.prebufferFmp4[0].time < now - defaultPrebufferDuration) {
            this.prebufferFmp4.shift()
          }

          // Emit event for any listeners
          this.events.emit('atom', atom)
        }
      } catch (error) {
        this.log.error(`Error in prebuffer parser: ${error}`, this.cameraName)
        // Attempt to restart the prebuffer if the parser fails
        this.restartPreBuffer()
      }
    })
    
    // Start listening for connections
    const fmp4Port = await listenServer(fmp4OutputServer, this.log)
    if (!fmp4Port) {
      throw new Error('Failed to start prebuffer server')
    }

    // Configure FFmpeg arguments
    const ffmpegOutput = [
      '-f',
      'mp4',
      ...vcodec,
      '-movflags',
      'frag_keyframe+empty_moov+default_base_moof',
      `tcp://127.0.0.1:${fmp4Port}`,
    ]

    const args: Array<string> = []
    args.push(...this.ffmpegInput.split(' '))
    args.push(...ffmpegOutput)

    const debug = true

    const stdioValue = debug ? 'pipe' : 'ignore'
    const cp = spawn(this.ffmpegPath, args, { env, stdio: stdioValue })

    // Set up process event handling
    cp.on('error', (err) => {
      this.log.error(`Prebuffer FFmpeg process error: ${err}`, this.cameraName)
    })
    
    cp.on('exit', (code, signal) => {
      this.log.error(`Prebuffer FFmpeg process exited with code ${code} and signal ${signal}`, this.cameraName)
      // Try to restart the prebuffer if it exits unexpectedly
      if (code !== 0 && !this.released) {
        this.restartPreBuffer()
      }
    })

    if (debug) {
      cp.stdout?.on('data', data => this.log.debug(data.toString(), this.cameraName))
      cp.stderr?.on('data', data => this.log.debug(data.toString(), this.cameraName))
    }

    // Store the session info
    prebufferSession = { server: fmp4OutputServer, process: cp }
    this.log.info('Prebuffer started successfully', this.cameraName)

    return prebufferSession
  }
  
  /**
   * Attempt to restart the prebuffer if it fails
   */
  private restartPreBuffer(): void {
    if (this.released) {
      return // Don't restart if we're shutting down
    }
    
    this.log.info('Attempting to restart prebuffer', this.cameraName)
    
    // Clean up existing session
    if (prebufferSession) {
      if (prebufferSession.process) {
        try {
          prebufferSession.process.kill('SIGKILL')
        } catch (e) {
          // Ignore errors while killing process
        }
      }
      if (prebufferSession.server) {
        try {
          prebufferSession.server.close()
        } catch (e) {
          // Ignore errors while closing server
        }
      }
    }
    
    // Reset prebuffer state
    this.ftyp = undefined as unknown as MP4Atom
    this.moov = undefined as unknown as MP4Atom
    this.isInitialized = false
    this.prebufferFmp4 = []
    
    // Try to restart after a short delay
    setTimeout(async () => {
      try {
        await this.startPreBuffer()
      } catch (error) {
        this.log.error(`Failed to restart prebuffer: ${error}`, this.cameraName)
      }
    }, 5000)
  }

  /**
   * Subscribe to prebuffer updates for direct streaming to HomeKit
   * @returns Subscriber ID that can be used to unsubscribe
   */
  subscribeToPreBuffer(callback: (data: Buffer) => void): number {
    const subscriberId = this.nextSubscriberId++
    this.recordingSubscribers.set(subscriberId, callback)
    this.log.debug(`New subscriber ${subscriberId} added to prebuffer`, this.cameraName)
    
    // If initialized, immediately send the MP4 headers to the new subscriber
    if (this.isInitialized && this.ftyp && this.moov) {
      const ftypBuffer = Buffer.concat([this.ftyp.header, this.ftyp.data])
      const moovBuffer = Buffer.concat([this.moov.header, this.moov.data])
      callback(Buffer.concat([ftypBuffer, moovBuffer]))
      
      // Also send recent fragments so recording can start immediately
      const now = Date.now()
      const recentFragments = this.prebufferFmp4
        .filter(fragment => fragment.time > now - 4000) // Last 4 seconds of data
        .map(fragment => Buffer.concat([fragment.atom.header, fragment.atom.data]))
      
      if (recentFragments.length > 0) {
        callback(Buffer.concat(recentFragments))
      }
    }
    
    return subscriberId
  }
  
  /**
   * Unsubscribe from prebuffer updates
   */
  unsubscribeFromPreBuffer(subscriberId: number): void {
    if (this.recordingSubscribers.has(subscriberId)) {
      this.recordingSubscribers.delete(subscriberId)
      this.log.debug(`Subscriber ${subscriberId} removed from prebuffer`, this.cameraName)
    }
  }
  
  /**
   * Notify all subscribers about new MP4 data
   */
  private notifySubscribers(atom: MP4Atom): void {
    if (this.recordingSubscribers.size === 0) {
      return // No subscribers
    }
    
    // Only send moof and mdat atoms to subscribers (and only when we've initialized)
    if (this.isInitialized && (atom.type === 'moof' || atom.type === 'mdat')) {
      const atomBuffer = Buffer.concat([atom.header, atom.data])
      
      for (const callback of this.recordingSubscribers.values()) {
        try {
          callback(atomBuffer)
        } catch (error) {
          this.log.error(`Error notifying prebuffer subscriber: ${error}`, this.cameraName)
        }
      }
    }
  }

  async getVideo(requestedPrebuffer: number): Promise<Array<string>> {
    const server = createServer((socket) => {
      server.close()

      const writeAtom = (atom: MP4Atom): void => {
        socket.write(Buffer.concat([atom.header, atom.data]))
      }

      let cleanup: () => void = (): void => {
        this.log.info('prebuffer request ended', this.cameraName)
        this.events.removeListener('atom', writeAtom)
        this.events.removeListener('killed', cleanup)
        socket.removeAllListeners()
        socket.destroy()
      }

      if (this.ftyp) {
        writeAtom(this.ftyp)
      }
      if (this.moov) {
        writeAtom(this.moov)
      }
      const now = Date.now()
      let needMoof = true
      for (const prebuffer of this.prebufferFmp4) {
        if (prebuffer.time < now - requestedPrebuffer) {
          continue
        }
        if (needMoof && prebuffer.atom.type !== 'moof') {
          continue
        }
        needMoof = false
        // console.log('writing prebuffer atom', prebuffer.atom);
        writeAtom(prebuffer.atom)
      }

      this.events.on('atom', writeAtom)
      this.activeAtomListeners++

      if (this.activeAtomListeners > 0 && this.activeAtomListeners % 10 === 0) {
        this.log.debug(`Active atom listeners: ${this.activeAtomListeners}`, this.cameraName)
      }

      cleanup = (): void => {
        this.log.info('prebuffer request ended', this.cameraName)
        this.events.removeListener('atom', writeAtom)
        this.events.removeListener('killed', cleanup)
        socket.removeAllListeners()
        socket.destroy()

        // Decrement the counter to keep track of active listeners
        this.activeAtomListeners = Math.max(0, this.activeAtomListeners - 1)
      }

      this.events.once('killed', cleanup)
      socket.once('end', cleanup)
      socket.once('close', cleanup)
      socket.once('error', cleanup)
    })

    // Close the server after 30 seconds or when the connection is complete
    const serverTimeout = setTimeout(() => {
      try {
        if (server.listening) {
          server.close()
          this.log.debug('Closing prebuffer server due to timeout', this.cameraName)
        }
      } catch (e) {
        // Ignore errors when closing server
      }
    }, 30000)
    
    // Also close the server when connection is complete
    server.once('close', () => {
      clearTimeout(serverTimeout)
      this.log.debug('Prebuffer server closed', this.cameraName)
    })

    const port = await listenServer(server, this.log)

    const ffmpegInput: Array<string> = [
      '-f',
      'mp4',
      '-i',
      `tcp://127.0.0.1:${port}`,
    ]

    return ffmpegInput
  }
  
  /**
   * Clean up resources when shutting down
   */
  releaseResources(): void {
    this.released = true
    this.events.emit('killed')
    
    // Clear event listeners
    this.events.removeAllListeners()
    
    // Clear subscriber callbacks
    this.recordingSubscribers.clear()
  }
}
