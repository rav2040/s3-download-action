import type { Readable } from "stream";

import { mkdir } from "fs/promises";
import { createWriteStream } from "fs";
import { join, dirname } from "path";
import { getInput, getMultilineInput, setFailed } from "@actions/core";
import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";

async function main() {
    try {
        const bucket = getInput("bucket", { required: true });
        const path = getMultilineInput("path", { required: true });
        const prefix = getInput("prefix");

        const s3 = new S3Client({});

        const keys = (await Promise.all(path.map(async (path) => {
            const listObjectsCommand = new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: [prefix, path].filter(Boolean).join("/"),
            });

            const response = await s3.send(listObjectsCommand);
            return (response.Contents ?? []).map((obj) => obj.Key ?? "").filter(Boolean);
        }))).flat();

        const uniqueKeys = Array.from(new Set(keys));

        await Promise.all(uniqueKeys.map(async (key) => {
            const filename = join(process.cwd(), prefix.length ? key.slice(key.indexOf("/") + 1) : key);

            const getObjectCommand = new GetObjectCommand({
                Bucket: bucket,
                Key: key,
            });
            const response = await s3.send(getObjectCommand);

            await mkdir(dirname(filename), { recursive: true });

            if (response.Body !== undefined) {
                const writeStream = createWriteStream(filename);
                (response.Body as Readable).pipe(writeStream);
            }

            console.info("Downloaded:", filename);
        }))
    } catch (err) {
        if (err instanceof Error) setFailed(err);
    }
}

main();
