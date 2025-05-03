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

export async function listenServer(server: Server, log: Logger): Promise<number> {
  let isListening = false
  while (!isListening) {
    const port = 10000 + Math.round(Math.random() * 30000)
    server.listen(port)
    try {
      await once(server, 'listening')
      isListening = true
      const address = server.address()
      if (address && typeof address === 'object' && 'port' in address) {
        return address.port
      }
      throw new Error('Failed to get server address')
    } catch (e: any) {
      log.error('Error while listening to the server:', e)
    }
  }
  // Add a return statement to ensure the function always returns a number
  return 0
}

export async function readLength(readable: Readable, length: number): Promise<Buffer> {
  if (!length) {
    return Buffer.alloc(0)
  }

  {
    const ret = readable.read(length)
    if (ret) {
      return ret
    }
  }

  return new Promise((resolve, reject) => {
    const r = (): void => {
      const ret = readable.read(length)
      if (ret) {
        // eslint-disable-next-line ts/no-use-before-define
        cleanup()
        resolve(ret)
      }
    }

    const e = (): void => {
      // eslint-disable-next-line ts/no-use-before-define
      cleanup()
      reject(new Error(`stream ended during read for minimum ${length} bytes`))
    }

    const cleanup = (): void => {
      readable.removeListener('readable', r)
      readable.removeListener('end', e)
    }

    readable.on('readable', r)
    readable.on('end', e)
  })
}

export async function* parseFragmentedMP4(readable: Readable): AsyncGenerator<MP4Atom> {
  while (true) {
    const header = await readLength(readable, 8)
    const length = header.readInt32BE(0) - 8
    const type = header.slice(4).toString()
    const data = await readLength(readable, length)

    yield {
      header,
      length,
      type,
      data,
    }
  }
}

export class RecordingDelegate implements CameraRecordingDelegate {
  async updateRecordingActive(active: boolean): Promise<void> {
    this.log.info(`Recording active status changed to: ${active}`, this.cameraName);
    
    if (active) {
      // Iniciar ou garantir que o pré-buffer esteja funcionando quando a gravação está ativa
      await this.startPreBuffer();
      
      // Se necessário, inicialize outros recursos para gravação
      this.isRecordingActive = true;
    } else {
      // A gravação não está mais ativa
      this.isRecordingActive = false;
      
      // Limpeza dos recursos de gravação, exceto se o pré-buffer deva continuar rodando
      if (this.activeRecordingSessions.size === 0 && !this.videoConfig?.prebuffer) {
        // Se não houver sessões ativas e o pré-buffer não estiver configurado para continuar,
        // podemos limpar os recursos
        if (this.preBufferSession) {
          this.log.debug('Stopping prebuffer session as recording is inactive', this.cameraName);
          if (this.preBufferSession.process) {
            this.preBufferSession.process.kill();
          }
          if (this.preBufferSession.server) {
            this.preBufferSession.server.close();
          }
          this.preBufferSession = undefined;
        }
      }
    }
    
    return Promise.resolve();
  }

  async updateRecordingConfiguration(configuration: CameraRecordingConfiguration): Promise<void> {
    this.log.info('Recording configuration updated', this.cameraName);
    
    // Armazene a configuração para uso em outras funções
    this.recordingConfiguration = configuration;
    
    if (configuration) {
      this.log.debug(`Audio codec: ${configuration.audioCodec.type}, ` +
        `Video resolution: ${configuration.videoCodec.resolution[0]}x${configuration.videoCodec.resolution[1]} @ ${configuration.videoCodec.resolution[2]}fps`, 
        this.cameraName);
      
      // Se a gravação estiver ativa, podemos precisar reiniciar o pré-buffer com as novas configurações
      if (this.isRecordingActive && this.videoConfig?.prebuffer) {
        // Reinicia o pré-buffer com as novas configurações, se necessário
        await this.restartPreBuffer();
      }
    }
    
    return Promise.resolve();
  }

  async *handleRecordingStreamRequest(streamId: number): AsyncGenerator<RecordingPacket, any, any> {
    this.log.info(`Recording stream request received for stream ID: ${streamId}`, this.cameraName);
    
    if (!this.videoConfig) {
      this.log.error('Video configuration is missing', this.cameraName);
      return;
    }

    try {
      // Start prebuffer if configured
      await this.startPreBuffer();
      
      // Get the recording configuration from the controller
      const recordingConfiguration = this.recordingConfiguration;
      
      if (!recordingConfiguration) {
        this.log.error('Recording configuration not available', this.cameraName);
        return;
      }
      
      // Generate fragments
      const fragmentsGenerator = this.handleFragmentsRequests(recordingConfiguration);
      
      // Process each fragment and convert to RecordingPacket
      for await (const fragment of fragmentsGenerator) {
        const packet: RecordingPacket = {
          data: fragment,
          isLast: false
        };
        
        // Track the latest active session info
        if (this.process) {
          const currentSession = {
            cp: this.process
          };
          this.activeRecordingSessions.set(streamId, currentSession);
        }
        
        yield packet;
      }
      
      // Send final packet
      yield {
        data: Buffer.alloc(0),
        isLast: true
      };
      
      // Remove from active sessions when complete
      this.activeRecordingSessions.delete(streamId);
      
    } catch (error) {
      this.log.error(`Error handling recording stream: ${error}`, this.cameraName);
      
      // Clean up in case of error
      this.activeRecordingSessions.delete(streamId);
      
      // Send final packet in case of error
      yield {
        data: Buffer.alloc(0),
        isLast: true
      };
    }
  }

  closeRecordingStream(streamId: number, reason: HDSProtocolSpecificErrorReason | undefined): void {
    this.log.info(`Recording stream closed for stream ID: ${streamId}, reason: ${reason ?? 'unknown'}`, this.cameraName);
    
    try {
      // Clean up any active recording sessions
      if (this.activeRecordingSessions && this.activeRecordingSessions.has(streamId)) {
        const session = this.activeRecordingSessions.get(streamId);
        if (session && session.cp) {
          // Matar o processo FFmpeg associado
          this.log.debug('Terminating FFmpeg process for recording', this.cameraName);
          session.cp.kill('SIGKILL');
        }
        
        // Remove from active sessions
        this.activeRecordingSessions.delete(streamId);
        this.log.debug(`Removed recording session for stream ID: ${streamId}`, this.cameraName);
      }
      
      // If this was the last active recording, we might want to stop the prebuffer
      if (this.activeRecordingSessions && this.activeRecordingSessions.size === 0 && !this.isRecordingActive) {
        this.log.debug('No active recording sessions remaining and recording is not active', this.cameraName);
        // Optionally stop prebuffer here if needed
      }
    } catch (error) {
      this.log.error(`Error closing recording stream: ${error}`, this.cameraName);
    }
  }

  private readonly hap: HAP
  private readonly log: Logger
  private readonly cameraName: string
  private readonly videoConfig?: VideoConfig
  private process!: ChildProcess

  private readonly videoProcessor: string
  readonly controller?: CameraController
  private preBufferSession?: Mp4Session
  private preBuffer?: PreBuffer

  private activeRecordingSessions: Map<number, {
    cp?: ChildProcess;
  }> = new Map();
  private recordingConfiguration?: CameraRecordingConfiguration;
  private isRecordingActive = false;

  constructor(log: Logger, cameraName: string, videoConfig: VideoConfig, api: API, hap: HAP, videoProcessor?: string) {
    this.log = log
    this.hap = hap
    this.cameraName = cameraName
    this.videoProcessor = videoProcessor || ffmpegPathString || 'ffmpeg'
    this.videoConfig = videoConfig

    api.on(APIEvent.SHUTDOWN, () => {
      if (this.preBufferSession) {
        this.preBufferSession.process?.kill()
        this.preBufferSession.server?.close()
      }
    })
  }

  async startPreBuffer(): Promise<void> {
    this.log.info(`start prebuffer ${this.cameraName}, prebuffer: ${this.videoConfig?.prebuffer}`)
    if (this.videoConfig?.prebuffer) {
      // looks like the setupAcessory() is called multiple times during startup. Ensure that Prebuffer runs only once
      if (!this.preBuffer) {
        this.preBuffer = new PreBuffer(this.log, this.videoConfig.source ?? '', this.cameraName, this.videoProcessor)
        if (!this.preBufferSession) {
          this.preBufferSession = await this.preBuffer.startPreBuffer()
        }
      }
    }
  }

  private async restartPreBuffer(): Promise<void> {
    this.log.debug('Restarting prebuffer with new configuration', this.cameraName);
    
    // Primeiro pare qualquer sessão de pré-buffer existente
    if (this.preBufferSession) {
      if (this.preBufferSession.process) {
        this.preBufferSession.process.kill();
      }
      if (this.preBufferSession.server) {
        this.preBufferSession.server.close();
      }
      this.preBufferSession = undefined;
    }
    
    // Reinicializar o pré-buffer
    if (this.preBuffer) {
      this.preBuffer = undefined; // Isso forçará a criação de um novo objeto PreBuffer
    }
    
    // Iniciar um novo pré-buffer
    await this.startPreBuffer();
  }

  async * handleFragmentsRequests(configuration: CameraRecordingConfiguration): AsyncGenerator<Buffer, void, unknown> {
    this.log.debug('video fragments requested', this.cameraName)

    const iframeIntervalSeconds = 4

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

    const profile = configuration.videoCodec.parameters.profile === H264Profile.HIGH
      ? 'high'
      : configuration.videoCodec.parameters.profile === H264Profile.MAIN ? 'main' : 'baseline'

    const level = configuration.videoCodec.parameters.level === H264Level.LEVEL4_0
      ? '4.0'
      : configuration.videoCodec.parameters.level === H264Level.LEVEL3_2 ? '3.2' : '3.1'

    const videoArgs: Array<string> = [
      '-an',
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

    const ffmpegInput: Array<string> = []

    if (this.videoConfig?.prebuffer) {
      const input: Array<string> = this.preBuffer ? await this.preBuffer.getVideo(configuration.mediaContainerConfiguration.fragmentLength ?? PREBUFFER_LENGTH) : []
      ffmpegInput.push(...input)
    } else {
      ffmpegInput.push(...(this.videoConfig?.source ?? '').split(' '))
    }

    this.log.debug('Start recording...', this.cameraName)

    const session = await this.startFFMPegFragmetedMP4Session(this.videoProcessor, ffmpegInput, audioArgs, videoArgs)
    this.log.info('Recording started', this.cameraName)

    const { socket, cp, generator } = session
    let pending: Array<Buffer> = []
    let filebuffer: Buffer = Buffer.alloc(0)
    try {
      for await (const box of generator) {
        const { header, type, length, data } = box

        pending.push(header, data)

        if (type === 'moov' || type === 'mdat') {
          const fragment = Buffer.concat(pending)
          filebuffer = Buffer.concat([filebuffer, Buffer.concat(pending)])
          pending = []
          yield fragment
        }
        this.log.debug(`mp4 box type ${type} and lenght: ${length}`, this.cameraName)
      }
    } catch (e) {
      this.log.info(`Recoding completed. ${e}`, this.cameraName)
      /*
            const homedir = require('os').homedir();
            const path = require('path');
            const writeStream = fs.createWriteStream(homedir+path.sep+Date.now()+'_video.mp4');
            writeStream.write(filebuffer);
            writeStream.end();
            */
    } finally {
      socket.destroy()
      cp.kill()
      // this.server.close;
    }
  }

  async startFFMPegFragmetedMP4Session(ffmpegPath: string, ffmpegInput: Array<string>, audioOutputArgs: Array<string>, videoOutputArgs: Array<string>): Promise<FFMpegFragmentedMP4Session> {
    return new Promise((resolve) => {
      const server = createServer((socket) => {
        server.close()
        async function* generator(): AsyncGenerator<MP4Atom> {
          while (true) {
            const header = await readLength(socket, 8)
            const length = header.readInt32BE(0) - 8
            const type = header.slice(4).toString()
            const data = await readLength(socket, length)

            yield {
              header,
              length,
              type,
              data,
            }
          }
        }
        const cp = this.process
        resolve({
          socket,
          cp,
          generator: generator(),
        })
      })

      listenServer(server, this.log).then((serverPort) => {
        const args: Array<string> = []

        args.push(...ffmpegInput)

        // args.push(...audioOutputArgs);

        args.push('-f', 'mp4')
        args.push(...videoOutputArgs)
        args.push('-fflags', '+genpts', '-reset_timestamps', '1')
        args.push(
          '-movflags',
          'frag_keyframe+empty_moov+default_base_moof',
          `tcp://127.0.0.1:${serverPort}`,
        )

        this.log.debug(`${ffmpegPath} ${args.join(' ')}`, this.cameraName)

        const debug = false

        const stdioValue = debug ? 'pipe' : 'ignore'
        this.process = spawn(ffmpegPath, args, { env, stdio: stdioValue })
        const cp = this.process

        if (debug) {
          if (cp.stdout) {
            cp.stdout.on('data', (data: Buffer) => this.log.debug(data.toString(), this.cameraName))
          }
          if (cp.stderr) {
            cp.stderr.on('data', (data: Buffer) => this.log.debug(data.toString(), this.cameraName))
          }
        }
      })
    })
  }
}
