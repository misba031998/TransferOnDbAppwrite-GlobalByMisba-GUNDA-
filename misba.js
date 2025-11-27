const http = require('http');
const url = require('url');
const admin = require('firebase-admin');
const sql = require('mssql');
const config = require('./config.js');
const { env } = require('process');

const TOKEN = process.env.SYNC_TOKEN;
const PORT = process.env.PORT || 3000;

// Initialize Firebase Admin SDK
admin.initializeApp({
  credential: admin.credential.cert(require('./serviceAccount.json')),
});

// =============================
// MSSQL CONNECTION POOL
// =============================
const getMSSQLConnectionPool = async (dbConfig) => {
  try {
    console.log(`MSSQLCONFIG for dbConfig`, JSON.stringify(dbConfig.user, null, 2));

    const pool = await sql.connect({
      user: dbConfig.user,
      password: dbConfig.password,
      server: dbConfig.server,
      port: dbConfig.port ? parseInt(dbConfig.port) : 1433,
      database: dbConfig.database,

      // ⭐ IMPORTANT FIX FOR LARGE INSERTS ⭐
      requestTimeout: 1200000,
      connectionTimeout: 300000,

      options: {
        encrypt: true,
        trustServerCertificate: true,
      },
    });

    await pool.request().query(`USE ${dbConfig.database}`);

    console.log('Connected to MSSQL successfully.');
    console.log(`Database in the connection pool: ${pool.config.database}`);

    return pool;
  } catch (error) {
    console.error('Error connecting to MSSQL:', error);
    throw new Error('MSSQL Connection Error');
  }
};

// =============================
// BULK INSERT (BATCHED for Stability)
// =============================
const bulkInsertDataNewMaxData = async (pool, data) => {
  try {
    const insertQuery = `
      INSERT INTO dbo.TBL_LOCATION 
      (UserId, Lattitude, Longitude, Address, Type, DateTime, Code, Installmentno)
      VALUES (@UserId, @Lattitude, @Longitude, @Address, @Type, @DateTime, @Code, @Installmentno)
    `;

    const BATCH_SIZE = 100;
    const totalBatches = Math.ceil(data.length / BATCH_SIZE);

    for (let i = 0; i < totalBatches; i++) {
      const batch = data.slice(i * BATCH_SIZE, (i + 1) * BATCH_SIZE);

      const transaction = new sql.Transaction(pool);
      await transaction.begin();

      try {
        for (const item of batch) {
          await transaction.request()
            .input('UserId', sql.NVarChar, item.UserId)
            .input('Lattitude', sql.NVarChar, item.Lattitude.toString())
            .input('Longitude', sql.NVarChar, item.Longitude.toString())
            .input('Address', sql.NVarChar, item.Address)
            .input('Type', sql.NVarChar, item.Type)
            .input('DateTime', sql.DateTime, new Date(item.DateTime))
            .input('Code', sql.NVarChar, item.Code)
            .input('Installmentno', sql.Int, parseInt(item.Installmentno))
            .query(insertQuery);
        }

        await transaction.commit();
        console.log(`Inserted batch ${i + 1}/${totalBatches} (${batch.length} rows)`);
      } catch (err) {
        console.log("Rolling back batch due to error...");
        await transaction.rollback();
        throw err;
      }
    }

    console.log(`Inserted ALL ${data.length} records into MSSQL.`);
  } catch (error) {
    console.error('Error during bulk insert into MSSQL:', error);
    throw new Error('MSSQL Bulk Insert Error');
  }
};

// =============================
// DELETE FIRESTORE
// =============================
const deleteFirestoreDataNewUsingLoop = async (firestoreConfig, firestoreDocs) => {
  try {
    const BATCH_SIZE = 500;
    console.log(`Attempting to delete ${firestoreDocs.length} documents from Firestore.`);

    for (let i = 0; i < firestoreDocs.length; i += BATCH_SIZE) {
      const batch = admin.firestore().batch();
      const chunk = firestoreDocs.slice(i, i + BATCH_SIZE);

      chunk.forEach(doc => {
        const docRef = admin.firestore().collection(firestoreConfig.collection).doc(doc.id);
        batch.delete(docRef);
      });

      await batch.commit();
      console.log(`Deleted batch of ${chunk.length} documents.`);
    }

    console.log(`Successfully deleted all ${firestoreDocs.length} documents from Firestore.`);
  } catch (error) {
    console.error('Error deleting Firestore documents:', error);
    throw new Error('Firestore Deletion Error');
  }
};

// =============================
// MAIN SYNC LOGIC
// =============================
const syncDataToMSSQL = async () => {
  console.log('Syncing data...');

  for (const firestoreDb in config.firestore) {
    const firestoreConfig = config.firestore[firestoreDb];
    let pool;
    try {
      console.log(`Processing Firestore DB: ${firestoreDb}`);
      const firestoreData = await admin.firestore()
        .collection(firestoreConfig.collection)
        .get();
      const data = firestoreData.docs.map(doc => doc.data());
      console.log(`Fetched ${data.length} records from Firestore DB ${firestoreDb}.`);
      if (data.length === 0) {
        console.log("No data to sync. Skipping...");
        continue;
      }
      pool = await getMSSQLConnectionPool(firestoreConfig.mssql);
      console.log(`MSSQL pool for DB ${firestoreDb} established.`);

      // 1️⃣ Insert into MSSQL
      await bulkInsertDataNewMaxData(pool, data);
      // 2️⃣ Delete Firestore Data
      await deleteFirestoreDataNewUsingLoop(firestoreConfig, firestoreData.docs);
    } catch (error) {
      console.error(`Error syncing Firestore DB ${firestoreDb} to MSSQL:`, error);
    } finally {
      if (pool && pool.close) {
        await pool.close();
        console.log(`Closed MSSQL pool for DB ${firestoreDb}.`);
      }
    }
  }
  console.log('Data syncing complete.');
};

// =============================
// HTTP SERVER (NON-BLOCKING)
// =============================
http.createServer(async (req, res) => {
  const parsedUrl = require('url').parse(req.url, true);
  if (parsedUrl.pathname === '/run-sync' && req.method === 'POST') {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1]; // Expect "Bearer <TOKEN>"
    if (token !== TOKEN) {
      res.writeHead(403, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, message: 'Forbidden: Invalid token daar nahi lagta' }));
      return;
    }
    // ✅ Token valid, start sync in background
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, message: 'Sync started in background' }));
    setImmediate(async () => {
      try {
        await syncDataToMSSQL();
      } catch (err) {
        console.error('Background sync error:', err);
      }
    });

    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Background worker running');
}).listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
// Run once on startup
syncDataToMSSQL().catch(err => console.error('Manual sync failed:', err));
