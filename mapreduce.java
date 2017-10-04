

public static class Map extends Mapper<Object, Text, Text, Text> {
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          String[] tableaux = value.toString().split(",");
          for (int i=1; i<tableaux.length ; i++)
          {
            context.write(new Text(i), new Text(tableaux[i]));
          }

       }
}
public static class Reduce extends Reducer<Text, Text, Text, Text {
       public void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException {

              for (IntWritable val : values) {
                  String row += val.toString() + ','
              }
              String res = row.substring(0, row.length() - 1)
              context.write(key, new Text(res));
     }
}
