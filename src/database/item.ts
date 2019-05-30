import * as _ from 'lodash';
import {Table} from './table'; // si ca fait une reference circulaire, créer un table manager et se référer à ça
import {EventEmitter} from 'events';

export class Item extends EventEmitter {
    private table: Table;
    private attributes: Object;

    public $add; //temp
    public $del; //temp
    // TODO: identity?

    constructor(attributes: Object, table: Table) {
        super();

        EventEmitter.call(this);

        this.table = table;
        this.attributes = this.setAttribute(attributes || {});
    }

    public setAttribute(attribute: Object): Item {
        this.attributes = _.merge({}, this.attributes, attribute);
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

    public async save(): Promise<any> {
        await this.table.create(this.attributes)
            .then(() => {

            })
            .catch((error) => {

            });
    }
}
