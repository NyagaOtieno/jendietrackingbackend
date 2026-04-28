import { initMariaDB } from "../services/mariaSync.service.js";

export async function initDb() {
  await initMariaDB();
}