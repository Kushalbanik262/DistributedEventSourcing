import { HdfsReaderWriter } from './hdfs-reader-writer.js';
import { error, info } from './logger.js';

const hdfs = new HdfsReaderWriter();
const hdfsPath = '/app_output.txt';
const sampleContent = `Hello from Node.js!\nTimestamp: ${new Date().toISOString()}\n`;

try {
  info('hdfs-demo', 'round_trip.started', { path: hdfsPath });
  await hdfs.writeFile(hdfsPath, sampleContent);

  const contents = await hdfs.readFile(hdfsPath, { encoding: 'utf8' });
  info('hdfs-demo', 'round_trip.completed', { path: hdfsPath, bytesRead: Buffer.byteLength(contents) });
} catch (cause) {
  error('hdfs-demo', 'round_trip.failed', { path: hdfsPath, cause });
  process.exitCode = 1;
}
