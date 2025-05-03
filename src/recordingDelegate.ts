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

  updateRecordingActive(active: boolean): Promise<void> {
    this.log.info(`Recording active status changed to: ${active}`, this.cameraName)
    return Promise.resolve()
  }

  public async updateRecordingConfiguration(configuration: CameraRecordingConfiguration): Promise<void> {
    this.log.info('Recording configuration updated', this.cameraName);
    this.recordingConfiguration = configuration;
    return Promise.resolve()
  }

  async *handleRecordingStreamRequest(streamId: number): AsyncGenerator<RecordingPacket, void, unknown> {
    this.streamId = streamId;
    this.log.info(`Recording stream request received for stream ID: ${streamId}`, this.cameraName)
    
    if (!this.videoConfig) {
      this.log.error('No video configuration for recording', this.cameraName)
      return
    }

    const configuration = this.recordingConfiguration;
    if (!configuration) {
      this.log.error('No recording configuration available', this.cameraName);
      return;
    }

    try {
      // Use the handleFragmentsRequests method to get the fragments
      for await (const fragment of this.handleFragmentsRequests(configuration)) {
        if (fragment && fragment.length > 0) {
          // Create the recording packet with the fragment
          const packet: RecordingPacket = {
            data: fragment,
            isLast: false // Set to true for the last packet in the recording
          }
          yield packet
        }
      } 
      yield {
        data: Buffer.alloc(0),   // sem payload
        isLast: true,            // sinaliza fim do stream
      };
    } catch (error) {
      this.log.error(`Error in recording stream: ${error}`, this.cameraName)
    }
  }

  closeRecordingStream(streamId: number, reason: HDSProtocolSpecificErrorReason | undefined): void {
    this.log.info(`Recording stream closed for stream ID: ${streamId}, reason: ${reason}`, this.cameraName)
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
  private recordingConfiguration?: CameraRecordingConfiguration;
  private streamId?: number;

  constructor(log: Logger, cameraName: string, videoConfig: VideoConfig, api: API, hap: HAP, videoProcessor?: string) {
    this.log = log
    this.hap = hap
    this.cameraName = cameraName
    this.videoConfig = videoConfig
    this.videoProcessor = videoProcessor || ffmpegPathString || 'ffmpeg'

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

  async *handleFragmentsRequests(configuration: CameraRecordingConfiguration): AsyncGenerator<Buffer, void, unknown> {
    this.log.debug('video fragments requested', this.cameraName);

    const iframeIntervalSeconds = 4;

    // --- profile e level para H264 ---
    const profile = configuration.videoCodec.parameters.profile === H264Profile.HIGH
      ? 'high'
      : configuration.videoCodec.parameters.profile === H264Profile.MAIN
        ? 'main'
        : 'baseline';

    const level = configuration.videoCodec.parameters.level === H264Level.LEVEL4_0
      ? '4.0'
      : configuration.videoCodec.parameters.level === H264Level.LEVEL3_2
        ? '3.2'
        : '3.1';

    const audioArgs = [
      '-codec:a', 'libfdk_aac',
      '-b:a', '128k',
      '-ar', '16k',
      '-ac', '1'
    ];

    const fps = configuration.videoCodec.resolution[2];
    const interval = iframeIntervalSeconds * fps;
    const maxWidth = this.videoConfig && this.videoConfig.maxWidth ? this.videoConfig.maxWidth : 0; 

    const filterArgs: string[] = [];
    if (maxWidth > 0) {
      // largura fixa em maxWidth, altura ajustada para manter aspect ratio
      filterArgs.push('-vf', `scale=${maxWidth}:-2`);
    }

    const videoArgs: string[] = [
      '-sn', '-dn',
      ...filterArgs,
      '-codec:v', 'libx264',
      '-pix_fmt', 'yuv420p',
      '-profile:v', profile,
      '-level:v', level,
      '-b:v', `${configuration.videoCodec.parameters.bitRate}k`,
      '-force_key_frames', `expr:eq(t,n_forced*${iframeIntervalSeconds})`,
      '-r', fps.toString(),
      '-g', interval.toString(),
      '-keyint_min', interval.toString(),
      '-force_key_frames',
        `expr:eq(t,n_forced*${iframeIntervalSeconds})`,
    ];

    // --- mapeamento de streams para incluir áudio ---
    const mapArgs = ['-map', '0:v', '-map', '0:a'];

    // monta ffmpegInput igual ao seu original
    const ffmpegInput: string[] = this.videoConfig?.prebuffer
      ? await this.preBuffer!.getVideo(configuration.mediaContainerConfiguration.fragmentLength ?? PREBUFFER_LENGTH)
      : (this.videoConfig!.source || '').split(' ');

    this.log.debug('Start recording...', this.cameraName);

    // chama o helper passando mapArgs + audioArgs antes de videoArgs
    const session = await this.startFFMPegFragmetedMP4Session(
      this.videoProcessor,
      ffmpegInput,
      [...mapArgs, ...audioArgs],
      videoArgs
    );
    this.log.info('Recording started', this.cameraName);

    // resto do generator igual ao que você já tinha
    const { socket, cp, generator } = session;
    let ffmpegExited = false;
    cp.once('exit', () => {
      ffmpegExited = true;
    });
    let pending: Buffer[] = [];
    let filebuffer = Buffer.alloc(0);

    try {
      for await (const box of generator) {
        if (ffmpegExited) {
          this.log.debug('FFmpeg exit detected — stopping fragment loop', this.cameraName);
          break;
        }
        const { header, type, data } = box;
        pending.push(header, data);

        if (type === 'moov' || type === 'mdat') {
          const fragment = Buffer.concat(pending);
          filebuffer = Buffer.concat([filebuffer, fragment]);
          pending = [];
          yield fragment;
        }
        this.log.debug(`mp4 box type ${type}`, this.cameraName);
      }
    } catch (e) {
      this.log.info(`Recording completed with error: ${e}`, this.cameraName);
    } finally {
      socket.destroy();
      cp.kill();
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

      const stderrChunks: Buffer[] = [];

      listenServer(server, this.log).then((serverPort) => {
        const args: Array<string> = []

        args.push(...ffmpegInput)

        args.push(...audioOutputArgs);

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

        if (cp.stderr) {
          cp.stderr.on('data', (chunk: Buffer) => stderrChunks.push(chunk));
        }

        cp.on('exit', (code: number | null, signal: string | null) => {
          const stderr = Buffer.concat(stderrChunks).toString();
          const reason = code && code !== 0
            ? `ffmpeg exit code ${code}` + (signal ? `, signal ${signal}` : '')
            : signal
              ? `killed by signal ${signal}`
              : undefined;

          if (reason) {
            this.log.error(reason + '\n' + stderr);
          }
        });

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
