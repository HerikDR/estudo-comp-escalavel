docker exec -it spark-master /opt/spark/bin/spark-submit \
  --class ExemploSparkMain \
  --master spark://spark-master:7077 \
  /opt/spark/submissions/aulasparkdemo_2.12-0.1.jar