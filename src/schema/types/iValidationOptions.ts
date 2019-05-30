import {ValidationPresences} from '../../database/types/validationPresences';

export interface IValidationOptions {
    abortEarly?:        boolean;
    convert?:           boolean;
    allowUnknown?:      boolean;
    skipFunctions?:     boolean;
    stripUnknown?:      boolean;
    language?:          Object;
    presence?:          ValidationPresences;
    strip?:             boolean;
    noDefaults?:        boolean;
}
