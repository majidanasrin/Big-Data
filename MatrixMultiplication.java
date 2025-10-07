import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

    // Mapper: Generates partial products (Key: C[i,j], Value: Matrix, k, value)
    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

        private int aRows; // M (Rows of A/C) - Used for B elements
        private int bCols; // N (Columns of B/C) - Used for A elements

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            // Retrieve dimensions from the driver
            aRows = conf.getInt("matrix.a.rows", 0);
            bCols = conf.getInt("matrix.b.columns", 0);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) 
                        throws IOException, InterruptedException {
            // Input format: MatrixID, RowIndex, ColIndex, Value
            String[] parts = value.toString().split(",");
            if (parts.length < 4) return;

            String matrixId = parts[0].trim();
            String i_or_k = parts[1].trim(); 
            String j_or_k = parts[2].trim();
            String val = parts[3].trim();

            if (matrixId.equalsIgnoreCase("A")) {
                // Element A[i][k]
                String i = i_or_k; // Row index (i)
                String k = j_or_k; // Inner index (k)
                
                // Propagate A[i][k] to all target cells C[i][j] (j=0 to N-1)
                for (int j = 0; j < bCols; j++) {
                    // Output Key: "i,j" (Target cell in C)
                    // Output Value: "A,k,value"
                    context.write(new Text(i + "," + j), new Text("A," + k + "," + val));
                }
            } else if (matrixId.equalsIgnoreCase("B")) {
                // Element B[k][j]
                String k = i_or_k; // Inner index (k)
                String j = j_or_k; // Col index (j)
                
                // Propagate B[k][j] to all target cells C[i][j] (i=0 to M-1)
                for (int i = 0; i < aRows; i++) { 
                    // Output Key: "i,j" (Target cell in C)
                    // Output Value: "B,k,value"
                    context.write(new Text(i + "," + j), new Text("B," + k + "," + val));
                }
            }
        }
    }

    // Reducer: Sums up partial products for a single cell C[i,j]
    public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                           throws IOException, InterruptedException {
            
            Map<String, Float> matrixA_values = new HashMap<>(); // Stores <k, A[i][k]>
            Map<String, Float> matrixB_values = new HashMap<>(); // Stores <k, B[k][j]>

            // 1. Group A and B elements by the inner index 'k'
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length < 3) continue;

                String matrix = parts[0];
                String k_index = parts[1]; 
                float element_value = Float.parseFloat(parts[2]);

                if (matrix.equals("A")) {
                    matrixA_values.put(k_index, element_value);
                } else if (matrix.equals("B")) {
                    matrixB_values.put(k_index, element_value);
                }
            }

            // 2. Compute C[i][j] = Sum over k (A[i][k] * B[k][j])
            float result = 0;
            for (String k_index : matrixA_values.keySet()) {
                Float a_val = matrixA_values.get(k_index);
                Float b_val = matrixB_values.get(k_index); 
                
                if (b_val != null) {
                    result += (a_val * b_val);
                }
            }

            // 3. Emit the final result
            context.write(key, new Text(String.valueOf(result)));
        }
    }

    public static void main(String[] args) throws Exception {
        // Requires 4 arguments: <input_path> <output_path> <A_rows> <B_cols>
        if (args.length != 4) { 
            System.err.println("Usage: MatrixMultiplication <input_path> <output_path> <A_rows> <B_cols>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        
        // Set the dimensions (dynamic input) via configuration
        int aRows = Integer.parseInt(args[2]);
        int bCols = Integer.parseInt(args[3]);
        
        conf.setInt("matrix.a.rows", aRows);
        conf.setInt("matrix.b.columns", bCols); 

        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatrixMultiplication.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
