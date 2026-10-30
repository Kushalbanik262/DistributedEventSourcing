import WebHDFS from 'webhdfs';
import {Readable} from "stream";

// Define the file System Connection
const hdfs = WebHDFS.createClient({
  user: 'root',
  host: 'localhost',
  port: 9870,
  path: '/webhdfs/v1'
});


const hdfsPath = '/app_output.txt';
const sampleContent = `Hello from Node.js inside Docker!\nTimestamp: ${new Date().toISOString()}\n`;


// Main Driver Function to Test
async function run(){
    console.log(`Starting to Write File...`);
    try{
        // Uplodad data to HDFS
         
        const readStream = Readable.from([sampleContent]);
        const writeStream = hdfs.createWriteStream(hdfsPath);

        await new Promise((resolve, reject) => {
            console.log(`Trying to write file...`);
            readStream.pipe(writeStream);
            writeStream.on('finish', resolve);
            writeStream.on('error', reject);
        });

         console.log(`File written successfully to HDFS at ${hdfsPath}`);

        // Read it back to verify
        console.log('Verifying by reading back from HDFS...');

        // Now Try to read the file on the same path
        hdfs.readFile(hdfsPath, (err, data) => {
            if (err) throw err;
            console.log(`Content from HDFS:\n${data.toString()}`);
        });
    }
    catch(err){
        console.error(`Error During HDFS Operation:${err}`);
    }
}



await run();