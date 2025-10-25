const admin = require('firebase-admin');
const sql = require('mssql');
const cron = require('node-cron');
const config = require('./config'); // Your dynamic configuration
const url = require('url');
//dummy port add
const http = require('http');
const PORT = process.env.PORT || 3000;
// Initialize Firebase Admin SDK
admin.initializeApp({
  credential: admin.credential.cert(require('./serviceAccount.json')),  // Your service account
});

// Function to connect to MSSQL databases with pooling
const getMSSQLConnectionPool = async (dbConfig) => {
  try {
    console.log(`MSSQLCONFIG for dbConfig`, JSON.stringify(dbConfig, null, 2));

    // Ensure the database context is explicitly set to the one in dbConfig
    const pool = await sql.connect({
      user: dbConfig.user,
      password: dbConfig.password,
      server: dbConfig.server,
      port: parseInt(dbConfig.port),
      database: dbConfig.database,
      options: {
        encrypt: true,
        trustServerCertificate: true,
      },
    });

    // Explicitly set the database context if needed
    await pool.request().query(`USE ${dbConfig.database}`);
    console.log('Connected to MSSQL successfully.');

    const actualDatabase = pool.config.database;
    console.log(`Database in the connection pool: ${actualDatabase}`);

    if (dbConfig.database === actualDatabase) {
      console.log(`The database specified in dbConfig (${dbConfig.database}) matches the database in the pool.`);
    } else {
      console.warn(`Database mismatch: dbConfig is for ${dbConfig.database}, but pool is connected to ${actualDatabase}`);
    }

    return pool;
  } catch (error) {
    console.error('Error connecting to MSSQL:', error);
    throw new Error('MSSQL Connection Error');
  }
};

// Bulk Insert Data into MSSQL
const bulkInsertData = async (pool, data) => {
  try {
    const request = pool.request();
    const insertQuery = `
      INSERT INTO dbo.TBL_LOCATION (UserId, Lattitude, Longitude, Address, Type, DateTime, Code, Installmentno)
      VALUES (@UserId, @Lattitude, @Longitude, @Address, @Type, @DateTime, @Code, @Installmentno)
    `;
    
    // Transaction for bulk insert
    const transaction = new sql.Transaction(pool);
    await transaction.begin();

    for (const item of data) {
      await transaction.request()
        .input('UserId', sql.NVarChar, item.UserId)
        .input('Lattitude', sql.NVarChar, item.Lattitude.toString())  // Convert latitude to string
        .input('Longitude', sql.NVarChar, item.Longitude.toString())  // Convert longitude to string
        .input('Address', sql.NVarChar, item.Address)
        .input('Type', sql.NVarChar, item.Type)
        .input('DateTime', sql.DateTime, new Date(item.DateTime))  // Ensure valid DateTime
        .input('Code', sql.NVarChar, item.Code)
        .input('Installmentno', sql.Int, parseInt(item.Installmentno))  // Parse installment number
        .query(insertQuery);
    }

    await transaction.commit();
    pool.close();
    console.log(`Inserted ${data.length} records into MSSQL.`);
  } catch (error) {
    console.error('Error during bulk insert into MSSQL:', error);
    throw new Error('MSSQL Bulk Insert Error');
  }
};

const bulkInsertDataNewMaxData = async (pool, data) => {
  try {
    const insertQuery = `
      INSERT INTO dbo.TBL_LOCATION (UserId, Lattitude, Longitude, Address, Type, DateTime, Code, Installmentno)
      VALUES (@UserId, @Lattitude, @Longitude, @Address, @Type, @DateTime, @Code, @Installmentno)
    `;

    const transaction = new sql.Transaction(pool);
    await transaction.begin();

    for (const item of data) {
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
    console.log(`Inserted ${data.length} records into MSSQL.`);
  } catch (error) {
    console.error('Error during bulk insert into MSSQL:', error);
    throw new Error('MSSQL Bulk Insert Error');
  }
};


// Function to delete Firestore data after successful insertion into MSSQL
const deleteFirestoreData = async (firestoreConfig, firestoreDocs) => {
  try {
    const batch = admin.firestore().batch();

    // Loop over documents and delete them
    firestoreDocs.forEach(doc => {
      const docRef = admin.firestore().collection(firestoreConfig.collection).doc(doc.id);
      batch.delete(docRef);
    });

    // Commit the batch delete operation
    await batch.commit();
    console.log(`Deleted ${firestoreDocs.length} documents from Firestore.`);
  } catch (error) {
    console.error('Error deleting Firestore documents:', error);
    throw new Error('Firestore Deletion Error');
  }
};

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

// Function to sync data from Firestore to MSSQL

const syncDataToMSSQL = async () => {
  console.log('Syncing data...');

  for (const firestoreDb in config.firestore) {
    const firestoreConfig = config.firestore[firestoreDb];
    let pool;

    try {
      console.log(`Processing Firestore DB: ${firestoreDb}`);

      const firestoreData = await admin.firestore().collection(firestoreConfig.collection).get();
      const data = firestoreData.docs.map(doc => doc.data());
      console.log(`Fetched ${data.length} records from Firestore DB ${firestoreDb}.`);

      const mssqlConfig = firestoreConfig.mssql;
      pool = await getMSSQLConnectionPool(mssqlConfig);
      console.log(`MSSQL pool for DB ${firestoreDb} established.`);

      await bulkInsertDataNewMaxData(pool, data);

      await deleteFirestoreDataNewUsingLoop(firestoreConfig, firestoreData.docs);

    } catch (error) {
      console.error(`Error syncing Firestore DB ${firestoreDb} to MSSQL:`, error);
    } finally {
      if (pool && pool.close) {
        await pool.close();  // ✅ Close MSSQL connection properly
        console.log(`Closed MSSQL pool for DB ${firestoreDb}.`);
      }
    }
  }

  console.log('Data syncing complete.');
};

const syncDataToMSSQLOld = async () => {
  console.log('Syncing data...');

  // Loop through all Firestore DBs defined in the config
  for (const firestoreDb in config.firestore) {
    const firestoreConfig = config.firestore[firestoreDb];

    try {
      console.log(`Processing Firestore DB: ${firestoreDb}`);

      // Fetch data from Firestore collection for the current DB
      const firestoreData = await admin.firestore().collection(firestoreConfig.collection).get();
      const data = firestoreData.docs.map(doc => doc.data());
      console.log(`Fetched ${data.length} records from Firestore DB ${firestoreDb}:`, data);

      // Ensure the MSSQL config is being correctly used for each Firestore DB
      const mssqlConfig = firestoreConfig.mssql;
      console.log(`MSSQL Configuration for ${firestoreDb}:`, JSON.stringify(mssqlConfig, null, 2));

      // Connect to MSSQL pool using the correct configuration for this Firestore DB
      const pool = await getMSSQLConnectionPool(mssqlConfig);

      console.log(`MSSQL pool for DB ${firestoreDb} established.`);

      // Bulk Insert data into MSSQL database
     //misba await bulkInsertData(pool, data);
      await bulkInsertDataNewMaxData(pool,data)

      // After data is inserted successfully, delete the Firestore data
     //misba await deleteFirestoreData(firestoreConfig, firestoreData.docs);

     await deleteFirestoreDataNewUsingLoop(firestoreConfig, firestoreData.docs)
    } catch (error) {
      console.error(`Error syncing Firestore DB ${firestoreDb} to MSSQL:`, error);
    }
  }

  console.log('Data syncing complete.');
};

// Optionally, call it once manually for immediate sync
syncDataToMSSQL().catch(err => console.error('Manual sync failed:', err));

http.createServer(async (req, res) => {
  const parsedUrl = url.parse(req.url, true);

  if (parsedUrl.pathname === '/run-sync' && req.method === 'POST') {
    try {
      console.log('Received manual sync trigger from Appwrite...');
      await syncDataToMSSQL();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, message: 'Data sync completed successfully.' }));
    } catch (error) {
      console.error('Manual sync failed:', error);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, error: error.message }));
    }
  } else {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Background worker is running.\n');
  }
}).listen(PORT, () => {
  console.log(`HTTP server running on port ${PORT}`);
});
