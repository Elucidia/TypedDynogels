import {LFService, Logger, LoggerFactory, LoggerFactoryOptions, LogGroupRule, LogLevel} from 'typescript-logging';
import {ILogArguments} from './iLogArguments';

export class Log {
    public static readonly ENTERING_FUNCTION_LOGGER_MESSAGE: string = 'Entering...';
    public static readonly EXIT_FUNCTION_LOGGER_MESSAGE: string = 'Exit...';
    private static readonly FACTORY: LoggerFactory = LFService.createNamedLoggerFactory('ServiceLoggerFactory', new LoggerFactoryOptions().addLogGroupRule(new LogGroupRule(new RegExp('.+'), LogLevel.Info)));
    private static readonly LOGGER_NAME: string = 'LOGGER';

    private static currentLogLevel: LogLevel = LogLevel.Info;
    private static loggerInstance = Log.GetLoggerFromFactory();

    private static interceptLogs: boolean = false;
    private static interceptedLogs: string[] = [];


    public static TraceObject(className: string, functionName: string, objectName: string, object: any) {
        Log.Trace(className, functionName, `${objectName}=${JSON.stringify(object)}`);
    }

    public static Trace(className: string, functionName: string, message: string, args?: ILogArguments[]) {
        if (Log.currentLogLevel === LogLevel.Trace) {
            const logString: string = Log.BuildStringToLog(className, functionName, message, args || null);
            if (Log.interceptLogs) {
                Log.interceptedLogs.push(`[TRACE] ${logString}`);
            } else {
                Log.loggerInstance.trace(`${logString}`);
            }
        }
    }

    public static Debug(className: string, functionName: string, message: string, args?: ILogArguments[]) {
        if (Log.currentLogLevel <= LogLevel.Debug) {
            const logString: string = Log.BuildStringToLog(className, functionName, message, args || null);
            if (Log.interceptLogs) {
                Log.interceptedLogs.push(`[DEBUG] ${logString}`);
            } else {
                Log.loggerInstance.debug(logString);
            }
        }
    }

    public static Enter(className: string, functionName: string, args?: any[]) {
        let counter = 0;
        Log.Trace(className, functionName, Log.ENTERING_FUNCTION_LOGGER_MESSAGE, args.map((arg) => {
            const logArg: ILogArguments = {
                name: counter.toString(),
                value: arg
            };
            counter++;
            return logArg;
        }));
    }

    public static Exit(className: string, functionName: string, returnValue?: any) {
        returnValue ? Log.Trace(className, functionName, Log.EXIT_FUNCTION_LOGGER_MESSAGE, [{name: 'Returning value', value: returnValue}]) :
            Log.Trace(className, functionName, Log.EXIT_FUNCTION_LOGGER_MESSAGE);
    }

    public static Info(className: string, functionName: string, message: string, args?: ILogArguments[]) {
        if (Log.currentLogLevel <= LogLevel.Info) {
            const logString: string = Log.BuildStringToLog(className, functionName, message, args || null);
            if (Log.interceptLogs) {
                Log.interceptedLogs.push(`[INFO] ${logString}`);
            } else {
                Log.loggerInstance.info(`${logString}`);
            }
        }
    }

    public static Success(className: string, functionName: string, message: string, args?: ILogArguments[]) {
        if (Log.currentLogLevel <= LogLevel.Debug) {
            const logString: string = Log.BuildStringToLog(className, functionName, message, args || null);
            if (Log.interceptLogs) {
                Log.interceptedLogs.push(`[SUCCESS] ${logString}`);
            } else {
                Log.loggerInstance.debug(`[SUCCESS] ${logString}`);
            }
        }
    }

    public static Warn(className: string, functionName: string, message: string, args?: ILogArguments[]) {
        if (Log.currentLogLevel <= LogLevel.Warn) {
            const logString: string = Log.BuildStringToLog(className, functionName, message, args || null);
            if (Log.interceptLogs) {
                Log.interceptedLogs.push(`[WARN] ${logString}`);
            } else {
                Log.loggerInstance.warn(`${logString}`);
            }
        }
    }

    public static Error(className: string, functionName: string, message: string, args?: ILogArguments[]) {
        if (Log.currentLogLevel <= LogLevel.Error) {
            const logString: string = Log.BuildStringToLog(className, functionName, message, args || null);
            if (Log.interceptLogs) {
                Log.interceptedLogs.push(`[ERROR] ${logString}`);
            } else {
                Log.loggerInstance.error(`${logString}`);
            }
        }
    }

    public static Fatal(className: string, functionName: string, message: string, args?: ILogArguments[]) {
        const logString: string = Log.BuildStringToLog(className, functionName, message, args || null);

        if (Log.interceptLogs) {
            Log.interceptedLogs.push(`[FATAL] ${logString}`);
        } else {
            Log.loggerInstance.fatal(`${logString}`);
        }
    }

    public static InterceptLogs() {
        Log.interceptLogs = true;
    }

    public static IsIntercepting(): boolean {
        return Log.interceptLogs;
    }

    public static ReleaseLogs(): string[] {
        let logsToReturn: string[] = Log.interceptedLogs;

        if (Log.interceptLogs) {
            Log.interceptLogs = false;
            logsToReturn = Log.interceptedLogs;
            Log.interceptedLogs = [];
        }

        return logsToReturn;
    }

    public static SetLogLevel(level: LogLevel) {
        Log.currentLogLevel = level;
        Log.loggerInstance = Log.GetLoggerFromFactory();
    }

    public static GetLogLevel(): LogLevel {
        return Log.currentLogLevel;
    }

    private static BuildStringToLog(className: string, functionName: string, message: string, args?: ILogArguments[]): string {
        let stringToShow: string = `[${className}.${functionName}] ${message}`;

        if (args) {
            args.forEach((argument) => {
                stringToShow += `.\n\t${argument.name}: ${JSON.stringify(argument.value) || 'null'}`;
            });
        }

        return stringToShow;
    }

    private static GetLoggerFromFactory(): Logger {
        Log.FACTORY.configure(new LoggerFactoryOptions().addLogGroupRule(new LogGroupRule(new RegExp('.+'), Log.currentLogLevel)));
        return Log.FACTORY.getLogger(Log.LOGGER_NAME);
    }
}
