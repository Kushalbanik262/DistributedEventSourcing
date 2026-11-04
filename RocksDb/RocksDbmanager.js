// This will help to manage RocksDB based Transactions 
import { RocksDatabase } from '@harperfast/rocksdb-js';
import { join } from 'path';
import { error , info, log, warn } from '../FileSystem/logger.js';

const dbpath = join(process.cwd(),"rocks_data");
let db;
const service = "SNAPSHORT";

async function init(){
    try{
        db = new RocksDatabase(dbpath,
            {
                compression: { algorithm: 'zstd', level: 3 }
            }
        );
    
        info(service,"Initializing",db);
    
        db.open();
        info(service,"INITIALIZATION",`success: ${db}`);
   }
   catch(error){
    //
    error(service,"INITIALIZATION",error);
    throw error; //re throw to catch via the primary caller
   }
}


// Write To Database 
export async function writeToDb(key,value){
    await db.put(key,{...value,time:Date.now()});
}

// Read all the snapsots for a specific user
export async function readFromDb(key){
    return await db.get(key);
}

// This reads a specific snapshot of a specific user saved through trhe state
async function readAllValuesForASpecificUser(){

}


init().catch(err=>{
    error(service,`DB Initialization Failed`,err);
});