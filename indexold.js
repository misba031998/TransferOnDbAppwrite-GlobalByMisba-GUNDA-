const admin = require('firebase-admin');
const sql = require('mssql');
const cron = require('node-cron');
const config = require('./config'); // Your dynamic configuration

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

// Function to sync data from Firestore to MSSQL
const syncDataToMSSQL = async () => {
  console.log('Syncing data...');

  // Loop through all Firestore DBs defined in the config
  for (const firestoreDb in config.firestore) {
    const firestoreConfig = config.firestore[firestoreDb];

    try {
      // Fetch data from Firestore collection
      const firestoreData = await admin.firestore().collection(firestoreConfig.collection).get();
      const data = firestoreData.docs.map(doc => doc.data());
      console.log(`Fetched ${data.length} records from Firestore DB ${firestoreDb}:`, data);

      const mssqlConfig = firestoreConfig.mssql;
      console.log(`MSSQLCONFIG for ${firestoreDb}:`, JSON.stringify(mssqlConfig, null, 2));

      // Connect to MSSQL pool
      const pool = await getMSSQLConnectionPool(mssqlConfig);

      console.log(`MSSQLCONFIG for POOL:`, JSON.stringify(pool, null, 2));

      // Bulk Insert data into MSSQL database
      await bulkInsertData(pool, data);

      // After data is inserted successfully, delete the Firestore data
      await deleteFirestoreData(firestoreConfig, firestoreData.docs);

      //pool.close();

    } catch (error) {
      console.error(`Error syncing Firestore DB ${firestoreDb} to MSSQL:`, error);
    }
  }

  console.log('Data syncing complete.');
};

// Schedule the function to run every 2 hours using node-cron
cron.schedule('0 */2 * * *', async () => {
  console.log('Running scheduled sync every 2 hours...');
  await syncDataToMSSQL();
});

// Optionally, call it once manually for immediate sync
syncDataToMSSQL().catch(err => console.error('Manual sync failed:', err));





/*const functions = require('firebase-functions');
const admin = require('firebase-admin');
const sql = require('mssql');
const config = require('./config');

admin.initializeApp({
  credential: admin.credential.cert(require('./serviceAccount.json')),  
});

// Function to connect to MSSQL databases
const getMSSQLConnection = async (dbConfig) => {
  try {
    return await sql.connect({
      user: dbConfig.user,
      password: dbConfig.password,
      server: dbConfig.server,
      database: dbConfig.database,
    });
  } catch (error) {
    console.error('Error connecting to MSSQL:', error);
    throw new Error('MSSQL Connection Error');
  }
};
exports.syncDataToMSSQL = functions.pubsub.schedule('every 2 hours').onRun(async (context) => {
  console.log('Syncing data every 2 hours');

  for (const firestoreDb in config.firestore) {
    const firestoreConfig = config.firestore[firestoreDb];

   
    const firestoreData = await admin.firestore().collection(firestoreConfig.collection).get();
    const data = firestoreData.docs.map(doc => doc.data());
    console.log(`Data from Firestore DB ${firestoreDb}:`, data);

    const mssqlConfig = firestoreConfig.mssql;

   
    const pool = await getMSSQLConnection(mssqlConfig);

     const insertQuery = `
    INSERT INTO dbo.TBL_LOCATION (UserId, Lattitude, Longitude, Address, Type, DateTime, Code, Installmentno)
    VALUES (@UserId, @Lattitude, @Longitude, @Address, @Type, @DateTime, @Code, @Installmentno)
  `;
  
  for (const item of data) {
    try {
      await pool.request()
        .input('UserId', sql.NVarChar, item.UserId)
        .input('Lattitude', sql.NVarChar, item.Lattitude.toString())  // Convert latitude to string
        .input('Longitude', sql.NVarChar, item.Longitude.toString())  // Convert longitude to string
        .input('Address', sql.NVarChar, item.Address)
        .input('Type', sql.NVarChar, item.Type)
        .input('DateTime', sql.DateTime, new Date(item.DateTime))  
        .input('Code', sql.NVarChar, item.Code)
        .input('Installmentno', sql.Int, parseInt(item.Installmentno)) 
        .query(insertQuery);

      console.log('Data inserted into MSSQL:', item);
   } catch (error) {
      console.error('Error inserting data into MSSQL:', error);
    }

  }

  console.log('Data syncing complete.');
});*/
