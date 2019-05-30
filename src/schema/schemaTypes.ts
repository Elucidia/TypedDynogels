import {array, string, ArraySchema, number, binary, StringSchema} from '@hapi/joi';
import {DynamoDBItemTypes} from '../database/types/dynamodbItemTypes';
import uuid = require('uuid');

export class SchemaTypes {
    public static TimeUUID(): StringSchema {
        return string().guid().default(() => uuid.v1(), 'uuid v1');
    }

    public static UUID(): StringSchema {
        return string().guid().default(() => uuid.v4(), 'uuid v4');
    }

    public static BinarySet(): ArraySchema {
        return array().items(binary(), string()).meta({dynamoDBType: DynamoDBItemTypes.BINARY_SET});
    }

    public static NumberSet(): ArraySchema {
        return array().items(number()).meta({dynamoDBType: DynamoDBItemTypes.NUMBER_SET});
    }

    public static StringSet(): ArraySchema {
        return array().items(string()).meta({dynamoDBType: DynamoDBItemTypes.STRING_SET});
    }
}
