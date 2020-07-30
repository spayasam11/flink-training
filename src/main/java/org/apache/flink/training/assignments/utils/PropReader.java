package org.apache.flink.training.assignments.utils;

import java.io.InputStream;
import java.util.Properties;

public  class PropReader {
    /*
    static {
        try {
            InputStream in = this.getClass().getClassLoader().getResourceAsStream("/application.properties");
            if (in != null) {
                props.load(in);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
*/
    private Properties props = null;
    public  void setProps()    {
            this.props = new Properties();
            try {
                InputStream in = getClass().getResourceAsStream("application.properties");
                if (in != null) {
                    props.load(in);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
    }

    public Properties getProps()    {
        if(props == null ) {
            setProps();
        }
        return props;
    }
}
