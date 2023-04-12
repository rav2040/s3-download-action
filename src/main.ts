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
            const filename = join(process.cwd(), prefix.length ? key.slice(key.indexOf("/") + 1) : key);
            const isDir = filename.at(-1) === "/";

            if (isDir) {
                await mkdir(filename, { recursive: true });
                console.info("Created directory:", filename);
                return;
            }

            const getObjectCommand = new GetObjectCommand({
                Bucket: bucket,
                Key: key,
            });
            const response = await s3.send(getObjectCommand);

            if (response.Body !== undefined) {
                await mkdir(dirname(filename), { recursive: true });
                const writeStream = createWriteStream(filename);
                await asyncPipe(response.Body as Readable, writeStream);
                filesDownloaded++;
                console.info("Downloaded:", filename);
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
