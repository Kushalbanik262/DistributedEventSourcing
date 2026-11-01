import WebHDFS from 'webhdfs';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { error, info, debug } from './logger.js';

/** Read and write files on HDFS through its WebHDFS endpoint. */
export class HdfsReaderWriter {
  constructor({
    user = process.env.HDFS_USER ?? 'root',
    host = process.env.HDFS_HOST ?? 'localhost',
    port = Number(process.env.HDFS_PORT ?? 9870),
    path = '/webhdfs/v1',
  } = {}) {
    this.client = WebHDFS.createClient({ user, host, port, path });
    info('hdfs', 'client.created', { host, port, webHdfsPath: path });
  }

  /** Write a string, Buffer, or readable/async iterable to an HDFS path. */
  async writeFile(hdfsPath, content) {
    if (typeof hdfsPath !== 'string' || !hdfsPath.startsWith('/')) {
      throw new TypeError('hdfsPath must be an absolute HDFS path');
    }

    const source = typeof content === 'string' || Buffer.isBuffer(content)
      ? Readable.from([content])
      : content;

    if (!source || (typeof source.pipe !== 'function' && typeof source[Symbol.asyncIterator] !== 'function')) {
      throw new TypeError('content must be a string, Buffer, readable stream, or async iterable');
    }

    const bytes = Buffer.isBuffer(content) ? content.length : typeof content === 'string' ? Buffer.byteLength(content) : undefined;
    info('hdfs', 'write.started', { path: hdfsPath, bytes });
    try {
      await pipeline(source, this.client.createWriteStream(hdfsPath));
      info('hdfs', 'write.completed', { path: hdfsPath, bytes });
    } catch (cause) {
      error('hdfs', 'write.failed', { path: hdfsPath, cause });
      throw cause;
    }
  }

  /** Append content to an HDFS file, creating its parent directory/file if needed. */
  async appendFile(hdfsPath, content) {
    if (typeof hdfsPath !== 'string' || !hdfsPath.startsWith('/')) {
      throw new TypeError('hdfsPath must be an absolute HDFS path');
    }

    const separator = hdfsPath.lastIndexOf('/');
    const parentPath = separator === 0 ? '/' : hdfsPath.slice(0, separator);
    info('hdfs', 'append.started', { path: hdfsPath });
    try {
      if (parentPath !== '/') {
        await new Promise((resolve, reject) => {
          this.client.mkdir(parentPath, (cause) => cause ? reject(cause) : resolve());
        });
      }

      const exists = await new Promise((resolve) => {
        this.client.exists(hdfsPath, resolve);
      });

      const source = typeof content === 'string' || Buffer.isBuffer(content)
        ? Readable.from([content])
        : content;

      if (!source || (typeof source.pipe !== 'function' && typeof source[Symbol.asyncIterator] !== 'function')) {
        throw new TypeError('content must be a string, Buffer, readable stream, or async iterable');
      }

      const bytes = Buffer.isBuffer(content) ? content.length : typeof content === 'string' ? Buffer.byteLength(content) : undefined;
      await pipeline(source, this.client.createWriteStream(hdfsPath, exists));
      info('hdfs', 'append.completed', { path: hdfsPath, bytes, previouslyExisted: exists });
    } catch (cause) {
      error('hdfs', 'append.failed', { path: hdfsPath, cause });
      throw cause;
    }
  }

  /** Read an HDFS file and return a Buffer, or text when an encoding is given. */
  async readFile(hdfsPath, { encoding } = {}) {
    if (typeof hdfsPath !== 'string' || !hdfsPath.startsWith('/')) {
      throw new TypeError('hdfsPath must be an absolute HDFS path');
    }

    info('hdfs', 'read.started', { path: hdfsPath, encoding: encoding ?? 'buffer' });
    try {
      const contents = await new Promise((resolve, reject) => {
        const chunks = [];
        const stream = this.client.createReadStream(hdfsPath);

        if (!stream || typeof stream.on !== 'function') {
          reject(new TypeError('WebHDFS did not return a readable stream'));
          return;
        }

        stream.on('data', (chunk) => {
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        });
        stream.once('end', () => resolve(Buffer.concat(chunks)));
        stream.once('error', reject);
      });

      info('hdfs', 'read.completed', { path: hdfsPath, bytes: contents.length });
      return encoding ? contents.toString(encoding) : contents;
    } catch (cause) {
      // silentry shallow
      info("hdfs",'read.issue',{'INFO':'File Not Found'});
      throw cause;
    }
  }

  /** Delete a file from HDFS. */
  async deleteFile(hdfsPath) {
    if (typeof hdfsPath !== 'string' || !hdfsPath.startsWith('/')) {
      throw new TypeError('hdfsPath must be an absolute HDFS path');
    }

    info('hdfs', 'delete.started', { path: hdfsPath });
    try {
      await new Promise((resolve, reject) => {
        this.client.unlink(hdfsPath, (cause) => cause ? reject(cause) : resolve());
      });
      info('hdfs', 'delete.completed', { path: hdfsPath });
    } catch (cause) {
      error('hdfs', 'delete.failed', { path: hdfsPath, cause });
      throw cause;
    }
  }
}
