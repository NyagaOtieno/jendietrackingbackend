
import { runMariaSync } from "../services/mariaSync.service.js";
import dotenv from 'dotenv';
dotenv.config();
async function testSync() {
  console.log("Starting manual Maria sync test...");
  
  const result = await runMariaSync();
  
  console.log("Sync result:", result);
  process.exit(0);
}

testSync();