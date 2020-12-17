import time
import os
import sys

import numpy as np
import tensorflow as tf
import tensorflow

print('TF version:')
print(tf.__version__)

pb_path = '/opt/storage/wangpei960/models/a1ec4b7e39cc11ebaab4040973ced990/models/sub_gpu_model'

with tf.Session(config=tf.ConfigProto(allow_soft_placement=True), graph=tf.Graph()) as sess:
    tf.saved_model.loader.load(sess, ['serve'], pb_path)
    graph = tf.get_default_graph()
    print([i.name for i in variables._all_saveable_objects()])
