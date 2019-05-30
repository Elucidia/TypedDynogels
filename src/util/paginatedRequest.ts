import {DynamoDB, AWSError} from 'aws-sdk';

export class PaginatedRequest {
    public static async Execute(self: any, runRequestFunc: Function): Promise<any> {
        let lastEvaluatedKey: DynamoDB.Key = null;
        let retry: boolean = true;
        const responses = [];

        if (lastEvaluatedKey) {
            self.startKey(lastEvaluatedKey);
        }

        await runRequestFunc(self.buildRequest())
            .then((success: any) => {

            })
            .catch((error: AWSError) => {
                if (error.retryable) {
                    retry = true;
                }
            })
    }
}
