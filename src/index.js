const AWS = require("aws-sdk");
const path = require("path");
const fs = require("fs-extra");
const { URL } = require("url");
const _ = require("lodash");
const rp = require("request-promise");
const Readable = require("stream").Readable;

const s3 = new AWS.S3();
const transcribe = new AWS.TranscribeService();

const log = o => console.log(JSON.stringify(o, null, 2));
const sleep = milliseconds =>
  new Promise(resolve => setTimeout(resolve, milliseconds));
const dataPath = filename => path.resolve(__dirname, "..", "data", filename);

const sourceMediaFileURLString =
  "https://www.voiptroubleshooter.com/open_speech/american/OSR_us_000_0010_8k.wav";

const transcribeWavFileExample = async () => {
  const projectDirectoryPath = path.resolve(__dirname, "..");
  const projectDirectoryBasename = path.basename(projectDirectoryPath);
  const prefix = projectDirectoryBasename;
  const bucketName = `${prefix}-${new Date().getTime()}`;
  const sourceMediaFileURL = new URL(sourceMediaFileURLString);
  const sourceMediaFileBasename = path.basename(sourceMediaFileURL.pathname);
  const localSourceMediaFilePath = dataPath(sourceMediaFileBasename);
  const localTranscriptFilePath = dataPath(
    `${sourceMediaFileBasename}.transcript.json`
  );
  const localTranscriptTextFilePath = dataPath(
    `${sourceMediaFileBasename}.transcript.txt`
  );

  // download source media file (.wav) locally
  await fs.remove(localSourceMediaFilePath);
  const sourceMediaFileResp = await rp(sourceMediaFileURLString, {
    resolveWithFullResponse: true, // wait until complete
    encoding: null // tell request that the data is binary
  });
  if (sourceMediaFileResp.statusCode !== 200) {
    throw new Error(
      `failed to download "${sourceMediaFileURLString}".  statusCode === ${
        sourceMediaFileResp.statusCode
      }`
    );
  }
  fs.outputFileSync(localSourceMediaFilePath, sourceMediaFileResp.body);

  // create temp bucket
  const createBucketParams = { Bucket: bucketName };
  log({ createBucketParams });
  const createBucketResp = await s3.createBucket(createBucketParams).promise();
  log({ createBucketResp });

  // upload local media file to bucket
  const putObjectParams = {
    Bucket: bucketName,
    Key: sourceMediaFileBasename,
    Body: fs.readFileSync(localSourceMediaFilePath)
  };

  log({ putObjectParams: _.omit(putObjectParams, ["Body"]) });
  const putObjectResp = await s3.putObject(putObjectParams).promise();
  log({ putObjectResp });

  const region = AWS.config.region; // default region. e.g. `us-east-1`;
  const mediaFileUri = `https://s3-${region}.amazonaws.com/${bucketName}/${sourceMediaFileBasename}`;

  // start transcription job
  const transcriptionJobName = `${bucketName}-${sourceMediaFileBasename}`;
  const startTranscriptionJobParams = {
    TranscriptionJobName: transcriptionJobName,
    LanguageCode: "en-US",
    Media: {
      MediaFileUri: mediaFileUri
    },
    MediaFormat: path.extname(localSourceMediaFilePath).split(".")[1],
    OutputBucketName: bucketName
  };
  log({ startTranscriptionJobParams });
  const startTranscriptionJobResp = await transcribe
    .startTranscriptionJob(startTranscriptionJobParams)
    .promise();
  log({ startTranscriptionJobResp });

  // poll transcription job status until reaches a completion status
  let getTranscriptionJobResp = null;
  const getTranscriptionJobParams = {
    TranscriptionJobName: transcriptionJobName
  };
  const completedStatuses = ["COMPLETED", "FAILED"];
  do {
    log({ getTranscriptionJobParams });
    getTranscriptionJobResp = await transcribe
      .getTranscriptionJob(getTranscriptionJobParams)
      .promise();
    log({ getTranscriptionJobResp });
    await sleep(3000);
  } while (
    !completedStatuses.includes(
      getTranscriptionJobResp.TranscriptionJob.TranscriptionJobStatus
    )
  );

  const transcriptFileUri =
    getTranscriptionJobResp.TranscriptionJob.Transcript.TranscriptFileUri;
  const transcriptFileURL = new URL(transcriptFileUri);
  const transcriptFileURLComponents = transcriptFileURL.pathname.split("/");

  await fs.remove(localTranscriptFilePath);

  // download transcription results from S3 to local file
  const getObjectParams = {
    Bucket: transcriptFileURLComponents[1],
    Key: transcriptFileURLComponents[2]
  };
  log({ getObjectParams });
  const getObjectResp = await s3.getObject(getObjectParams).promise();
  log({ getObjectResp: _.omit(getObjectResp, ["Body"]) });
  await fs.outputFile(localTranscriptFilePath, getObjectResp.Body);
  const transcriptResultJsonString = new Buffer(getObjectResp.Body).toString(
    "utf8"
  );
  const transcriptResult = JSON.parse(transcriptResultJsonString);

  // write the transcription text to a local file
  const transcriptText = transcriptResult.results.transcripts
    .map(ts => ts.transcript)
    .join("\n");

  await fs.outputFile(localTranscriptTextFilePath, transcriptText);

  // clean up

  // list all files in temp bucket
  const listObjectsV2Params = { Bucket: bucketName };
  log({ listObjectsV2Params });
  const listObjectsV2Resp = await s3
    .listObjectsV2(listObjectsV2Params)
    .promise();
  log({ listObjectsV2Resp });

  // delete all files in temp bucket
  const deleteObjectsParams = {
    Bucket: bucketName,
    Delete: {
      Objects: listObjectsV2Resp.Contents.map(item => ({ Key: item.Key }))
    }
  };
  log({ deleteObjectsParams });
  const deleteObjectsResp = await s3
    .deleteObjects(deleteObjectsParams)
    .promise();
  log({ deleteObjectsResp });

  // delete temp bucket
  const deleteBucketParams = { Bucket: bucketName };
  log({ deleteBucketParams });
  const deleteBucketResp = await s3.deleteBucket(deleteBucketParams).promise();
  log({ deleteBucketResp });
};

(async () => {
  await transcribeWavFileExample();
})();
