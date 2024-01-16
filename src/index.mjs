import AWS from 'aws-sdk';
import fs from 'fs';
import path, { dirname } from 'path';
import { fileURLToPath } from 'url';
import axios from 'axios';

const s3 = new AWS.S3({
    accessKeyId: process.env.AWS_IAM_ACCESS_KEY,
    secretAccessKey: process.env.AWS_IAM_SECRET_KEY,
    region: process.env.AWS_BUCKET_REGION
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const UNISWAP_API_ENDPOINT = process.env.UNISWAP_API_BASE_URL;

export const handler = async (event, context) => {
    let response = {
        statusCode: 404,
        body: JSON.stringify({
            Error: 'No Appropriate Routes Found'
        }),
    };

    // METHOD: GET
    if (event.path == "/api/test") {
        response = {
            statusCode: 200,
            body: JSON.stringify({
                message: UNISWAP_API_ENDPOINT
            })
        }
    }

    // METHOD: GET
    // endpoint to update the 15th day datset, body should include the poolAddress and the other predictors
    if (event.path == "/api/addNewDataset") {
        const usdcETH = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640";
        const fetchedData = await apiQuery(usdcETH);
        if (fetchedData) {
            const {data: destructuredData} = fetchedData;
            const {pool: poolsArray} = destructuredData;
            const {poolDayData: poolDayDataArray, id: poolId} = poolsArray;
            const filePath = csvHandler(poolId, poolDayDataArray);
            
            const formattedDay = formDay();

            // datasets are stored in poolAddress subdirectory
            const objectKey = `${usdcETH}/${formattedDay}.csv`;

            // upload to s3 bucket
            uploadDataset(filePath, objectKey);
            response = {
                statusCode: 200,
                body: JSON.stringify(fetchedData)
            }
        } else {
            const errorMessage = { error: "Invalid Pool Address entered"};
            response = {
                statusCode: 401,
                body: JSON.stringify(errorMessage)
            }
        }
    }
}

// utility functions
// form the graphQL body for the past latest 14 days
const formulateBody = (poolAddress) =>  {
    return `query {
    pool(id: "${poolAddress}") {
        poolDayData(last: 14) {
            date,
            open,
            high,
            low,
            close,
            volumeUSD,
            feesUSD,
            txCount,
        }
    }
}`
}

const formDay = () => {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const formattedDate = `${year}-${month}-${day}`;
    return formattedDate;
}

// standard method to make axios API call
const apiQuery = async (poolAddress) => {
    try {    
        const { data } = await axios.post(UNISWAP_API_ENDPOINT, {
            query: formulateBody(poolAddress),
        });
        return data;
    } catch (error) {
        console.error('Error fetching data:', error);
    }
}

// conversion of dataset to .csv
const csvHandler = async (poolId, poolDayDataArray) => {
    const csv = new ObjectsToCsv(poolDayDataArray);
    const filePath = path.join(__dirname, `./assets/datasets/${poolId}.csv`);
    await csv.toDisk(filePath);
    return filePath;
}

// upload dataset to s3 bucket using aws-sdk
const uploadDataset = async (filePath, newFileNameKey) => {
    const finalisedFilePath = await filePath;
    const fileStream = fs.createReadStream(finalisedFilePath);
    fileStream.on('error', (err) => {
        console.log('File Error: ', err);
    })

    const params = {
        Bucket: process.env.AWS_BUCKET_NAME,
        Key: newFileNameKey,
        Body: fileStream
    }

    s3.upload(params, (err, data) => {
        if (err) {
            console.log('Error: ', err);
            return;
        }
    })
    console.log("Upload Successful!")
}