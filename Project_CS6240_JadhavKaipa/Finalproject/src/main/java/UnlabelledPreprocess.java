public class UnlabelledPreprocess {

    /*
     * This class is for pre-processing the unlabelled data to make it ready for
     * use in the classification spark program. The library used, MLLib, requires
     * the data to be of the following format:
     *      <label> <index>:<attribute>
     *  The label here is the unique identifier for the record being processed.
     *  The label as to be a Number so the leading character is removed. The index
     *  has to start from 1. The attribute as to be some Number format; String is
     *  not allowed.
     *
     *  The columns chosen for building the model are explained in the report for
     *  the project. For each column entry, parse the column data only is it is
     *  not "?". Also ignore the first line in the input data set which starts with
     *  "SAMPLING_EVENT_ID".
     *
     *  The difference between pre-processing for labelled data and unlabelled
     *  data is that the <label> in labelled data comes from the label assigned
     *  in the training data, while the <label> in unlabelled data is the identifier
     *  for purpose of labelling it using the trained model.
     */

    public static String unlabelledPreprocess(String line) {

        StringBuilder sb = new StringBuilder();

        // ignore the first line of the input data
        if (line.contains("SAMPLING_EVENT_ID"))
            return null;

        // split the entries based on "," separator since file is a csv file
        String[] entries = line.split(",");

        // if there is a "?" assigned to the data we are interested in
        if (entries[26].equals("?")) {
            int count = 1, i = 2; // house keeping
            sb.append(entries[0].substring(1)); // identifier
            sb.append(" ");

            // output only the relevant labels needed for training
            while (i < 968) {
                if (!entries[i].equals("?") && !entries[i].equals("X") && count != 13) {
                    sb.append(count);
                    sb.append(":");
                    sb.append(entries[i]);
                    sb.append(" ");
                }
                count++;
                i++;
                if (i == 8)
                    i = 955;
            }
            return sb.toString();
        } else {
            return null;
        }

    }

}
