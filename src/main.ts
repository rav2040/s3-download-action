import { getInput, setFailed } from "@actions/core";
import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";

const PATH_SPLIT_REGEX = /\s+(?=([^"]*"[^"]*")*[^"]*$)/g;

async function main() {
    try {
        const bucket = getInput("bucket");
        const path = getInput("path");
        const prefix = getInput("prefix");

        const paths = path.split(PATH_SPLIT_REGEX).filter(Boolean);
        const uniquePaths = Array.from(new Set(paths));

        const s3 = new S3Client({});

        await Promise.all(uniquePaths.map(async (path) => {

            const listObjectsCommand = new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: [prefix, path].filter(Boolean).join("/"),
            });

            const response = await s3.send(listObjectsCommand);

            console.info(response);
        }))
    } catch (err) {
        if (err instanceof Error) setFailed(err);
    }
}

main();
