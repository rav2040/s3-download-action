import { writeFile } from "fs/promises";
import { join } from "path";
import { getInput, setFailed } from "@actions/core";
import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";

const PATH_SPLIT_REGEX = /\s+(?=([^"]*"[^"]*")*[^"]*$)/g;

async function main() {
    try {
        const bucket = getInput("bucket");
        const path = getInput("path");
        const prefix = getInput("prefix");

        const paths = path.split(PATH_SPLIT_REGEX).filter(Boolean);
        const uniquePaths = Array.from(new Set(paths));

        const s3 = new S3Client({});

        const keys = (await Promise.all(uniquePaths.map(async (path) => {
            const listObjectsCommand = new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: [prefix, path].filter(Boolean).join("/"),
            });

            const response = await s3.send(listObjectsCommand);
            return (response.Contents ?? []).map((obj) => obj.Key ?? "").filter(Boolean);
        }))).flat();

        const uniqueKeys = Array.from(new Set(keys));

        await Promise.all(uniqueKeys.map(async (key) => {
            const getObjectCommand = new GetObjectCommand({
                Bucket: bucket,
                Key: key,
            });

            const response = await s3.send(getObjectCommand);

            if (response.Body !== undefined) {
                const filename = join(process.cwd(), prefix.length ? key.slice(key.indexOf("/") + 1) : key);
                const data = await response.Body.transformToByteArray();
                await writeFile(filename, data);
                console.info("Downloaded:", filename);
            }
        }))
    } catch (err) {
        if (err instanceof Error) setFailed(err);
    }
}

main();
