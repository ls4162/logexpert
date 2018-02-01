package insight.engineer;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by sulei on 1/12/18.
 */
public class LogParser implements Serializable {
    public final SimpleDateFormat FORMAT = new SimpleDateFormat(
            "d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    /**
     * parse datetime string
     *
     * @param String
     * @return Date
     * @throws ParseException
     */
    private Date parseDateFormat(String string) {
        Date parse = null;
        try {
            parse = FORMAT.parse(string);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return parse;
    }

    /**
     * parse line
     *
     * @param String
     * @return String
     */
    public String parse(String line) {
        String ip = parseIP(line);
        String time = parseTime(line);
        String url = parseURL(line);
        String status = parseStatus(line);
        String traffic = parseTraffic(line);

        return ip + "," + time + "," + url ;
    }


    private String parseTraffic(String line) {
        final String trim = line.substring(line.lastIndexOf("\"") + 1)
                .trim();
        String traffic = trim.split(" ")[1];
        return traffic;
    }

    private String parseStatus(String line) {
        final String trim = line.substring(line.lastIndexOf("\"") + 1)
                .trim();
        String status = trim.split(" ")[0];
        return status;
    }

    private String parseURL(String line) {
        final int first = line.indexOf("\"");
        final int last = line.lastIndexOf("\"");
        String url = line.substring(first + 1, last);
        final int start = url.lastIndexOf("/");
        String page = url.substring(start + 1);
        return page;
    }

    private String parseTime(String line) {
        final int first = line.indexOf("[");
        final int last = line.indexOf("+0800]");
        String time = line.substring(first + 1, last).trim();
        Date date = parseDateFormat(time);
        return Long.toString(date.getTime());
    }

    private String parseIP(String line) {
        String ip = line.split("- -")[0].trim();
        return ip;
    }
}