const express = require("express");
const redis = require("redis");
// require("dotenv").config();
const AWS = require("aws-sdk");
const multer = require("multer");
// const { exec } = require("child_process");
const util = require("util");
const exec = util.promisify(require("child_process").exec);
// const zlib = require('zlib');
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const objectHash = require("object-hash");
const cors = require("cors");
// S3 setup
const bucketName = "group106assignment2";

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  sessionToken: process.env.AWS_SESSION_TOKEN,
  region: "ap-southeast-2",
});
const s3 = new AWS.S3();
const objectKey = "text.json";

const app = express();
const corsOptions = {
  origin: "*",
  optionsSuccessStatus: 200,
};

app.use(cors(corsOptions));
const port = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, "build")));

const redisClient = redis.createClient();
redisClient.connect().catch((err) => {
  console.log(err);
  console.log("Cant connect to Redis");
});

// process.on("SIGINT", () => {
//   console.log("Caught interrupt signal, closing Redis client connection...");
//   redisClient.quit(() => {
//     console.log("Redis client connection closed.");
//     process.exit(0); // Exit the Node.js process
//   });
// });

app.use(express.json());
app.use(express.static("uploads"));

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    // Define the directory where you want to save the uploaded files
    cb(null, "./uploads");
  },
  filename: (req, file, cb) => {
    // Define the file name for the saved file
    cb(null, file.originalname);
  },
});

const upload = multer({ storage });

async function uploadFileToS3(fileName) {
  console.log("***uploadFileToS3: ", fileName);
  const compressedFileName = `${fileName}.bz2`;
  const fileStream = fs.createReadStream(`./uploads/${compressedFileName}`);

  const params = {
    Bucket: bucketName,
    Key: compressedFileName,
    Body: fileStream,
  };

  try {
    await s3.putObject(params).promise();
    console.log("JSON file uploaded successfully.");
  } catch (err) {
    console.error("Error uploading JSON file:", err);
  }

  fs.unlink(`./uploads/${compressedFileName}`, (err) => {
    if (err) console.error("Error deleting local file:", err);
  });
}

async function getCacheObjectFromS3() {
  console.log("***getCacheObjectFromS3:***");
  const params = {
    Bucket: bucketName,
    Key: objectKey,
  };

  try {
    const data = await s3.getObject(params).promise();
    // Parse JSON content
    const parsedData = JSON.parse(data.Body.toString("utf-8"));
    console.log("Parsed JSON data:", parsedData);
    return parsedData;
  } catch (err) {
    console.error("Error: No object/File in S3");
  }
}

async function uploadCacheObjectToS3(data) {
  console.log(`***uploadCacheObjectToS3: ${data}`);

  const params = {
    Bucket: bucketName,
    Key: objectKey,
    Body: JSON.stringify(data), // Convert JSON to string
    ContentType: "application/json", // Set content type
  };

  try {
    await s3.putObject(params).promise();
    console.log("JSON file uploaded successfully.");
  } catch (err) {
    console.error("Error uploading JSON file:", err);
  }
}

async function generateHash(filePath) {
  console.log(`***generateHash: ${filePath}`);

  return new Promise((resolve, reject) => {
    const hash = crypto.createHash("md5");
    const input = fs.createReadStream(filePath);

    input.on("data", (chunk) => {
      hash.update(chunk);
    });

    input.on("end", () => {
      const hashValue = hash.digest("hex");
      console.log(`MD5 Hash Value for the file: ${hashValue}`);
      resolve(hashValue); // Resolve the promise with the hashValue
    });

    input.on("error", (err) => {
      reject(err); // Reject the promise in case of an error
    });
  });
}

async function generatePresignedUrl(key) {
  console.log(`***generatePresignedUrl: ${key}`);
  console.log(`***Type***: ${typeof key}`);
  const compressedFileName = `${key}.bz2`;
  const params = {
    Bucket: bucketName,
    Key: compressedFileName,
    Expires: 3600, // URL expiration time in 1 hour
    ResponseContentDisposition: `${key}.bz2`,
  };

  return s3.getSignedUrl("getObject", params);
}

async function saveToRedis(key, value) {
  console.log(`***saveToRedis: key=${key} value=${value}`);
  // console.log("Value:", value);

  // Use 'let' to declare persistedCache as it needs to be mutable
  let persistedCache = {};

  // Save the key-value pair to Redis
  await redisClient.set(key, JSON.stringify(value));

  // Retrieve the persistedCache from Amazon S3
  persistedCache = await getCacheObjectFromS3();

  if (persistedCache) {
    // Update the cache object with the new key-value pair
    persistedCache[key] = value;
  } else {
    // Initialize the cache object with the new key-value pair
    persistedCache = {
      [key]: value,
    };
  }

  // Upload the updated cache object to Amazon S3
  await uploadCacheObjectToS3(persistedCache);
}

async function flushRedis() {
  console.log(`***flushRedis***`);
  await redisClient.flushAll();

  // await redisClient.flushAll((err, reply) => {
  //   if (err) {
  //     console.error("Error flushing all data:", err);
  //   } else {
  //     console.log("Successfully flushed all data:", reply);
  //   }
  // });
}

async function saveMultipleToRedis(data) {
  console.log(`***saveMultipleToRedis: ${data}`);
  if (data) {
    for (const key of Object.keys(data)) {
      const value = data[key];
      await redisClient.set(key, JSON.stringify(value));
    }
  }
}

async function checkRedis(fileHash) {
  console.log(`***checkRedis: ${fileHash}`);
  let redisValue = await redisClient.get(fileHash);
  if (redisValue) {
    console.log("**File coming from redis**");
    redisValue = JSON.parse(redisValue);
    const presignedUrl = await generatePresignedUrl(redisValue);
    return presignedUrl;
  } else {
    const persistedCache = await getCacheObjectFromS3();
    await flushRedis();
    await saveMultipleToRedis(persistedCache);
    redisValue = await redisClient.get(fileHash);
    if (redisValue) {
      console.log("**File coming from S3**");
      redisValue = JSON.parse(redisValue);

      const presignedUrl = await generatePresignedUrl(redisValue);
      return presignedUrl;
    } else {
      return null;
    }
  }
}

// async function compressFile(fileName) {
//   await exec("bzip2 ./uploads/" + fileName, function (err, stdout, stderr) {
//     if (err) {
//       console.error(stderr);
//       return false;
//     }
//     console.log(stdout);
//     return true;
//   });
// }

// Define a route to handle file uploads

async function compressFile(fileName) {
  console.log(`***compressFile: ${fileName}`);
  try {
    console.log("bzip2 ./uploads/" + fileName);
    const { stdout, stderr } = await exec("bzip2 ./uploads/" + fileName);
    console.log(stdout);
    return true;
  } catch (error) {
    console.error("Error: " + error.stderr);
    return false;
  }
}

app.post("/upload", upload.single("files"), async (req, res) => {
  
  console.log("------------------------------------");
  const originalFIleName = req.file.originalname;
  const filePath = `./uploads/${originalFIleName}`;
  const fileHash = await generateHash(filePath);
  console.log(typeof fileHash);
  try {
    // Check if the file is in Redis
    const isPresignedUrl = await checkRedis(fileHash);
    if (isPresignedUrl) {
      return res.json({ download_link: isPresignedUrl });
    }
    // Compress the file
    const isFileCompressed = await compressFile(originalFIleName);
    console.log("isFileCompressed: " + isFileCompressed);

    if (!isFileCompressed) {
      return res.status(500).send("Failed to compress the file. Sorry!");
    }

    // Upload to S3
    await uploadFileToS3(originalFIleName);

    // Save to Redis
    await saveToRedis(fileHash, originalFIleName);

    // Generate and send the pre-signed URL
    const presignedUrl = await generatePresignedUrl(originalFIleName);
    res.json({ download_link: presignedUrl });
  } catch (err) {
    console.log("Error:", err);
    res.status(500).send("An error occurred.");
  }
});

app.get("/", (req, res) => {
  console.log("hellosas!!");
  res.sendFile(path.join(__dirname, "build", "index.html"));
});

app.listen(port, async () => {
  console.log(`Server is running on port ${port}`);

  // const s3 = new AWS.S3();

  //creat the bucket
  async function createS3bucket(bucketName) {
    try {
      await s3.createBucket({ Bucket: bucketName }).promise();
      console.log(`Created bucket: ${bucketName}`);
    } catch (err) {
      if (err.statusCode === 409) {
        console.log(`Bucket already exists: ${bucketName}`);
      } else {
        console.log(`Error creating bucket: ${err}`);
      }
    }
  }

  (async () => {
    await createS3bucket(bucketName);
  })();
});
