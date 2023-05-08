package org.opengauss.portalcontroller.constant;

public interface Default {
    interface Check{
        interface Sink{
            String QUERY_DOP = "8";
            String MIN_IDLE = "10";
            String MAX_ACTIVE = "20";
            String INITIAL_SIZE = "5";
            String TIME_PERIOD = "1";
            String NUM_PERIOD = "1000";
        }
        interface Source{
            String QUERY_DOP = "8";
            String MIN_IDLE = "10";
            String MAX_ACTIVE = "20";
            String INITIAL_SIZE = "5";
            String TIME_PERIOD = "1";
            String NUM_PERIOD = "1000";
        }
        String RULES_ENABLE = "false";
        int TABLE_AMOUNT = 0;
        int ROW_AMOUNT = 0;
        int COLUMN_AMOUNT = 0;
    }
    interface Chameleon{
        interface Override{
            int AMOUNT = 0;

        }
    }
}
