import * as _ from 'lodash';
import {Table} from './table'; // si ca fait une reference circulaire, créer un table manager et se référer à ça
import {EventEmitter} from 'events';
import {AttributeValue, UpdateItemOutput, DeleteItemOutput} from 'aws-sdk/clients/dynamodb';
import {Log} from '../util/log';
import {IUpdateItemOptions} from './types/iUpdateItemOptions';
import {IDeleteItemOptions} from './types/iDeleteItemOptions';

export class Item extends EventEmitter {
    private table: Table;
    private attributes: Object;

    public $add; //temp
    public $del; //temp
    // TODO: identity?

    /**
     *
     * @param attributes In the form of PutItemInput Item value
     * @param table
     */
    constructor(attributes: Object, table: Table) {
        super();

        EventEmitter.call(this);

        this.table = table;
        this.attributes = this.setAttribute(attributes || {});
    }

    public setAttribute(attribute: Object): Item {
        this.attributes = {...this.attributes, ...attribute};
        return this;
    }

    public getAttribute(attributeKey: string): any {
        if (attributeKey) {
            return this.attributes[attributeKey];
        }
    }

    public getAttributes(): Object {
        return this.attributes;
    }

    //TODO: modifier quand Table.putItem va retourner un PutItemOutput à place
    public async save(): Promise<any> {
        let resultToReturn: Object;
        await this.table.putItem(this)
            .then((result: Object) => {
                this.setAttribute(result);
                resultToReturn = result;
            })
            .catch((error) => {
                Log.Error(Item.name, 'save', 'Unable to perform putItem operation');
                return Promise.reject(error);
            });
        return Promise.resolve(resultToReturn);
    }

    public async update(options?: IUpdateItemOptions): Promise<UpdateItemOutput | any> {
        let resultToReturn: UpdateItemOutput;
        await this.table.updateItem(this.attributes, options)
            .then((result: UpdateItemOutput) => {
                if (result) {
                    this.setAttribute(result.Attributes);
                }
                resultToReturn = result;
            })
            .catch((error: any) => {
                Log.Error(Item.name, 'update', 'Unable to perform updateItem operation');
                return Promise.reject(error);
            });
        return Promise.resolve(resultToReturn);
    }

    public async delete(options?: IDeleteItemOptions): Promise<DeleteItemOutput | any> {
        let resultToReturn: DeleteItemOutput;
        await this.table.deleteItem(this.attributes, null, options)
            .then((success: DeleteItemOutput) => {
                resultToReturn = success;
            })
            .catch((error: any) => {
                Log.Error(Item.name, 'update', 'Unable to perform deleteItem operation');
                return Promise.reject(error);
            });
        return Promise.resolve(resultToReturn);
    }

    public attributeToJSON(): Object {
        return _.cloneDeep(this.attributes);
    }

    // TODO: why having 2 functions that are the **** same
    public toPlainObject(): Object {
        return this.attributeToJSON();
    }
}
