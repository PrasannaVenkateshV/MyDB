import java.util.*;
import java.io.*;


/**
 * @Author: Prasanna V Varadan, pras.venkatesh@gmail.com
 * MyDB is an in-memory DB, and conforms to the below specification.
 * https://www.thumbtack.com/challenges/simple-database
 *
 * Prerequisite: JDK 1.8
 * To compile: javac MyDB.Java
 * To run: java MyDB
 */

public class MyDB {

    /**
     * Initial capacity for the DB.
     */
    private static final int INITIAL_DB_SIZE = 20;

    /**
     * Initial DB load factor.
     */
    private static final float INITIAL_DB_LOAD_FACTOR = 0.7f;

    /**
     * The default size of the Transaction DB Map.
     */
    private static final int TRANSACTION_MAP_SIZE = 5;

    /**
     * The Load Factor for the transaction map
     */
    private static final float TRANSACTION_MAP_LOAD_FACTOR = 0.9f;

    /**
     * The NO TRANSACTION OUTPUT String
     */
    private static final String NO_TRANSACTION_OUTPUT = "NO TRANSACTION";

    /**
     * Null Output String
     */
    private static final String NULL_OUTPUT = "NULL";

    /**
     * TO_BE_DELETED
     */
    private  static final String TO_BE_DELETED = "TO_BE_DELETED";

    /**
     * MyDB - The Key Value Map. considered the in-memory DB.
     */
    private final static Map<String, String> myDBMap = new HashMap<>(INITIAL_DB_SIZE, INITIAL_DB_LOAD_FACTOR);

    /**
     * numEqualToMap - Map for faster computation of num of occurances of a value in the DB.
     */
    private final static Map<String, Integer> numEqualToMap  = new HashMap<>();

    /**
     * transactionStack - stack of transactions
     */
    private final static Stack<Map<String, String>> transactionStack = new Stack<>();

    /**
     * Main method - MyDB
     * @param args - args
     */
    public static void main(String[] args) {
        String input, output;
        processOutput("Started MyDB");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            while((input=br.readLine())!=null) {
                try {
                    if(input.length() > 0) {
                        output = processCommand(input);
                        processOutput(output);
                    }
                } catch (Exception e) {
                    processOutput("INVALID INPUT: " + input);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    /**
     * process the command
     * @param input - a line from stdin.
     * @return - command output
     */
    private static String processCommand(String input) {
        String[] inputs = input.split(" ");
        String commandString = inputs[0].toUpperCase();
        String keyString = inputs.length > 1 ? inputs[1] : null;
        String valueString = inputs.length > 2 ? inputs[2] : null;
        MyDBCommands command = MyDBCommands.valueOf(commandString);
        if (command == null) {
            throw new IllegalArgumentException("Not a valid Command");
        }
        Optional<String> commandOutput= command.execute(Optional.ofNullable(keyString), Optional.ofNullable(valueString));
        return commandOutput.isPresent() ? commandOutput.get(): null;
    }

    /**
     * process output
     * @param output - output to be printed.
     */
    private static void processOutput(String output) {
        if(output != null){
            System.out.println(output);
        }
    }

    /**
     * Enum of valid commands that MyDB can accept.
     */
    enum MyDBCommands {
        //(command name, Documentation)
        BEGIN ("BEGIN", "Open a new transaction block. Transaction blocks can be nested; a BEGIN can be issued inside of an existing block.") {
            public Optional<String> execute(Optional<String> key, Optional<String> value) {
                MyDBService.beginTransaction();
                return Optional.empty();
            }
        },
        END("END", "Exit the program. Your program will always receive this as its last command.") {
            public Optional<String> execute(Optional<String> key, Optional<String> value) {
                System.exit(0);
                return Optional.empty();
            }
        },
        COMMIT("COMMIT", "Close all open transaction blocks, permanently applying the changes made in them. Print nothing if successful, or print NO TRANSACTION if no transaction is in progress.") {
            public Optional<String> execute(Optional<String> key, Optional<String> value) {
                return MyDBService.commitTransaction();
            }
        },
        ROLLBACK("ROLLBACK", "Undo all of the commands issued in the most recent transaction block, and close the block. Print nothing if successful, or print NO TRANSACTION if no transaction is in progress.") {
            public Optional<String> execute(Optional<String> key, Optional<String> value) {
                return MyDBService.rollbackTransaction();
            }
        },
        GET("GET", "Print out the value of the variable name, or NULL if that variable is not set.") {
            public Optional<String> execute(Optional<String> key, Optional<String> value) {
                return Optional.of(MyDBService.get(getOptionalData(key)));
            }
        },
        SET("SET", "Set the variable name to the value value. Neither variable names nor values will contain spaces.") {
            public Optional<String> execute(Optional<String> key, Optional<String> value) {
                MyDBService.set(getOptionalData(key), getOptionalData(value));
                return Optional.empty();
            }
        },
        UNSET("UNSET", "Unset the variable name, making it just like that variable was never set."){
            public Optional<String> execute(Optional<String> key, Optional<String> value) {
                MyDBService.unset(getOptionalData(key));
                return Optional.empty();
            }
        },
        NUMEQUALTO("NUMEQUALTO", "Print out the number of variables that are currently set to value. If no variables equal that value, print 0."){
            public Optional<String> execute(Optional<String> key, Optional<String> value) {
                return Optional.of(String.valueOf(MyDBService.getNumEqualTo(getOptionalData(key))));
            }
        };

        private final String commandName;
        private final String commandDescription;

        MyDBCommands(String commandName, String commandDescription) {
            this.commandName = commandName;
            this.commandDescription = commandDescription;
        }

        abstract Optional<String> execute(Optional<String> key, Optional<String> Value);

        private static String getOptionalData(Optional<String> data){
            return data.isPresent() ? data.get(): "";
        }

    }


    /**
     * MyDBService - the service class for MyDB.
     */
    public static class MyDBService {

        /**
         * set value directly in the DB or into transaction memory based on the context
         * @param key - key
         * @param value - value
         */
        public static void set(String key, String value) {
            Map<String, String> mapInContext = getMapByContext();
            if(mapInContext.get(key) != null || myDBMap.get(key) != null) {
                String  currentValueTemp = mapInContext.get(key);
                String currentValue = currentValueTemp != null ? currentValueTemp : myDBMap.get(key);
                if(!currentValue.equals(value)) {

                    updateNumEqualTo(currentValue, false);
                    updateNumEqualTo(value, true);
                }
            }
            else {
                updateNumEqualTo(value, true);
            }
            mapInContext.put(key, value);
        }

        /**
         * gets the value mapped to the key, returns null if none found.
         * @param key - key
         * @return value
         */
        public static String get(String key) {
            Map<String, String> mapInContext = getMapByContext();
            String value = mapInContext.get(key);
            if(isTransactionScope() && mapInContext.get(key) == null) {
                value = myDBMap.get(key);
            }
            if (value == null || value.equals(TO_BE_DELETED)){
              value = NULL_OUTPUT;
            }
            return value;
        }

        /**
         * unset a record(key, value) in the db.
         * @param key - key
         */
        public static void unset(String key) {
            Map<String, String> mapInContext = getMapByContext();
            String value = mapInContext.get(key);
            if (isTransactionScope()) {
                mapInContext.remove(key);
                if(myDBMap.get(key) != null) {
                    value = myDBMap.get(key);
                    updateNumEqualTo(value, true);
                    mapInContext.put(key, TO_BE_DELETED);
                }
            } else {
                mapInContext.remove(key);
            }
            updateNumEqualTo(value, false);
        }


        /**
         * getNumEqualTo - get the 'numEqualTo' value of a given string from the db.
         * @param value - value
         * @return - numEqualTo
         */
        public static int getNumEqualTo(String value) {
            int count = numEqualToMap.get(value) != null ? numEqualToMap.get(value) : 0;
            return count;
        }

        /**
         * begin the transaction
         */
        public static void beginTransaction() {
            if (transactionStack.empty()) {
                transactionStack.push(new HashMap<>(TRANSACTION_MAP_SIZE, TRANSACTION_MAP_LOAD_FACTOR));
            }
            else {
                transactionStack.push(new HashMap<>(transactionStack.peek()));
            }
        }

        /**
         * rollback the transaction
         */
        public static Optional<String>  rollbackTransaction() {
            if(!isTransactionScope()){
                return Optional.of(NO_TRANSACTION_OUTPUT);
            }
            Map<String, String> transactionMap = transactionStack.pop();
            Map<String, String> parentTransactionMap  = transactionStack.empty() ? null : transactionStack.peek();
            for (Map.Entry<String, String> entry : transactionMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                String valueInMyDB = myDBMap.get(key);
                String valueInDB, valueInParentTransactionMap = null;
                if(parentTransactionMap != null) {
                    valueInParentTransactionMap = parentTransactionMap.get(key);
                }
                valueInDB = valueInParentTransactionMap != null ? valueInParentTransactionMap : valueInMyDB;
                if(value.equals(TO_BE_DELETED)) {
                    value = myDBMap.get(key);
                    updateNumEqualTo(value, true);
                }
                if(valueInDB != null && !valueInDB.equals(value)) {
                    updateNumEqualTo(valueInDB, true);
                }
                updateNumEqualTo(value, false);
            }
            return Optional.empty();
        }

        /**
         * commit the transaction
         */
        public static Optional<String> commitTransaction() {
            if (!isTransactionScope()) {
                return Optional.of(NO_TRANSACTION_OUTPUT);
            }
            Map<String, String> transactionMap = transactionStack.pop();
            transactionStack.clear();
            for (Map.Entry<String, String> entry : transactionMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if(value.equals(TO_BE_DELETED)) {
                    myDBMap.remove(key);
                }
                else {
                    myDBMap.put(key, value);
                }
            }
            return Optional.empty();
        }

        /**
         * increments or decrements the numEqualTo Value of a given key.
         * @param key - key
         * @param isIncrement - boolean to increment or decrement.
         */
        private static void updateNumEqualTo(String key, boolean isIncrement) {
            int incrementValue = isIncrement ? 1 : -1;
            int numEqualTo = 0;
            if(numEqualToMap.get(key) != null) {
                numEqualTo = numEqualToMap.get(key) + incrementValue;
            } else if (isIncrement) {
                numEqualTo = 1;
            }
            if(numEqualTo > 0) {
                numEqualToMap.put(key, numEqualTo);
            } else {
                numEqualToMap.remove(key);
            }
        }

        /**
         * identifies if the context is Transaction or atomic updates and return the right Object(Map)
         * @return - The appropriate Map based on the context
         */
        private static Map<String,String> getMapByContext() {
            return isTransactionScope() ? transactionStack.peek() : myDBMap;
        }

        /**
         * returns is there is a transaction in progress.
         * @return - is Transaction scope or not
         */
         private static boolean isTransactionScope() {
           return !transactionStack.empty();
        }
    }
}

