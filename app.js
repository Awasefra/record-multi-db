import mysql from "mysql2/promise";
import fs from "fs";
import dotenv from "dotenv";

dotenv.config();

async function countAllTablesFromDBs() {
  // Buat folder logs jika belum ada
  if (!fs.existsSync("logs")) {
    fs.mkdirSync("logs");
  }

  const conn = await mysql.createConnection({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
  });

  const DB_NAMES = process.env.DB_LIST.split(",").map((db) => db.trim());

  const allLogs = {};
  const epoch = Date.now();

  for (const dbName of DB_NAMES) {
    console.log(`\n🔍 Memproses database: ${dbName}`);

    const [tables] = await conn.query(
      `
            SELECT table_name AS table_name 
            FROM information_schema.tables 
            WHERE table_schema = ?
            `,
      [dbName]
    );

    if (tables.length === 0) {
      console.log(
        `⚠️ Tidak ada akses atau tidak ada tabel untuk DB: ${dbName}`
      );
      continue;
    }

    const resultPerDB = [];

    for (const tbl of tables) {
      const tableName = tbl.table_name;

      try {
        const [countRow] = await conn.query(
          `SELECT COUNT(*) AS total FROM \`${dbName}\`.\`${tableName}\``
        );

        resultPerDB.push({
          table: tableName,
          total: countRow[0].total,
          error: "",
        });
      } catch (error) {
        resultPerDB.push({
          table: tableName,
          total: "",
          error: error.message,
        });
      }
    }

    allLogs[dbName] = resultPerDB;

    // PRINT per DB
    console.log(`\n=== ${dbName} ===`);
    console.table(resultPerDB);

    //
    // 🔥 SAVE CSV PER DB
    //
    const fileDB = `logs/log_${epoch}_${dbName}.csv`;

    const csvHeader = "table,total,error\n";
    const csvBody = resultPerDB
      .map((row) => `${row.table},${row.total},${row.error}`)
      .join("\n");

    fs.writeFileSync(fileDB, csvHeader + csvBody);

    console.log(`📁 CSV per-DB disimpan: ${fileDB}`);
  }

  await conn.end();

  //
  // 🔥 SAVE CSV ALL (GABUNGAN)
  //
  const fileAll = `logs/log_${epoch}_ALL.csv`;

  const csvRows = ["database,table,total,error"];

  for (const dbName of Object.keys(allLogs)) {
    allLogs[dbName].forEach((row) => {
      csvRows.push(`${dbName},${row.table},${row.total},${row.error}`);
    });
  }

  fs.writeFileSync(fileAll, csvRows.join("\n"));

  console.log(`\n📁 CSV gabungan disimpan: ${fileAll}`);
}

countAllTablesFromDBs();
