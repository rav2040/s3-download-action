import type { Readable, Writable } from "stream";

import { mkdir } from "fs/promises";
import { createWriteStream } from "fs";
import { join, posix, dirname } from "path";
import { getInput, getMultilineInput, setFailed } from "@actions/core";
import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({});

async function main() {
    try {
        const bucket = getInput("bucket", { required: true });
        const path = getMultilineInput("path", { required: true });
        const prefix = getInput("prefix");

        const keys = (await Promise.all(path.map(async (path) => {
            return listS3Objects(bucket, posix.join(prefix, path));
        }))).flat();

        // Filter out directories that are common prefixes.
        const uniqueKeys = Array.from(new Set(keys))
            .filter((a, i, arr) => {
                return a.at(-1) !== "/" || !arr.some((b, j) => i !== j && b.startsWith(a) && b.length > a.length)
            });

        let filesDownloaded = 0;

        await Promise.all(uniqueKeys.map(async (key) => {
            const filepath = join(process.cwd(), key.slice(prefix.length));

            if (key.at(-1) === "/") {
                // Is directory
                await mkdir(filepath, { recursive: true });
                console.info("Downloaded:", filepath);
                return;
            }

            const getObjectCommand = new GetObjectCommand({
                Bucket: bucket,
                Key: key,
            });
            const response = await s3.send(getObjectCommand);

            if (response.$metadata.httpStatusCode === 200 && response.Body !== undefined) {
                await mkdir(dirname(filepath), { recursive: true });
                const writeStream = createWriteStream(filepath);
                await asyncPipe(response.Body as Readable, writeStream);
                filesDownloaded++;
                console.info("Downloaded:", filepath);
            }
        }));

        console.log("### Total files downloaded:", filesDownloaded, "###");
    } catch (err) {
        if (err instanceof Error) setFailed(err);
    }
}

async function listS3Objects(bucket: string, prefix: string, continuationToken?: string): Promise<string[]> {
    const listObjectsCommand = new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken
    });

    const response = await s3.send(listObjectsCommand);
    const result = (response.Contents ?? []).map((obj) => obj.Key ?? "").filter(Boolean);

    if (response.IsTruncated) {
        return result.concat(await listS3Objects(bucket, prefix, response.NextContinuationToken));
    }

    return result;
}

function asyncPipe(readStream: Readable, writeStream: Writable) {
    return new Promise<void>((resolve, reject) => {
        readStream.on("error", reject);
        writeStream.on("error", reject);
        writeStream.on("close", resolve);
        readStream.pipe(writeStream);
    });
}

main();
