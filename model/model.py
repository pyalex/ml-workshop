# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Example code for TensorFlow Wide & Deep Tutorial using tf.estimator API."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import shutil
import sys
from random import shuffle

import tensorflow as tf
from tensorflow.python.saved_model import tag_constants


_CSV_COLUMN_DEFAULTS = [[0], [''], [0], [''], [0], [''], [''], [''], [''], [''],
                        [0], [0], [0], [''], ['']]

parser = argparse.ArgumentParser()

parser.add_argument(
    '--model_dir', type=str, default='/Users/oleksiimo/projects/public/data/models',
    help='Base directory for the model.')

parser.add_argument(
    '--model_type', type=str, default='wide',
    help="Valid model types: {'wide', 'deep', 'wide_deep'}.")

parser.add_argument(
    '--train_epochs', type=int, default=1, help='Number of training epochs.')

parser.add_argument(
    '--epochs_per_eval', type=int, default=1,
    help='The number of training epochs to run between evaluations.')

parser.add_argument(
    '--batch_size', type=int, default=40, help='Number of examples per batch.')

parser.add_argument(
    '--train_data', type=str, default='/Users/oleksiimo/projects/public/data/features4/',
    help='Path to the training data.')

parser.add_argument(
    '--test_data', type=str, default='/Users/oleksiimo/projects/public/data/features4/',
    help='Path to the test data.')

_NUM_EXAMPLES = {
    'train': 100,
    'validation': 50,
}


def build_model_columns():
    """Builds a set of wide and deep feature columns."""
    # Continuous columns
    body_word_count = tf.feature_column.numeric_column('body_word_count')
    subject_word_count = tf.feature_column.numeric_column('subject_word_count')
    amount_of_participants = tf.feature_column.numeric_column('amount_of_participants')
    no_in_thread = tf.feature_column.numeric_column('no_in_thread')
    thread_has_previous_answer = tf.feature_column.numeric_column('thread_has_previous_answer')
    originator_in_cc = tf.feature_column.numeric_column('originator_in_cc')

    contact_title = tf.feature_column.categorical_column_with_vocabulary_list(
        'contact_title', [
            'Director', 'Manager'])

    wide_columns = [
        #contact_title,
        subject_word_count,
        amount_of_participants,
        no_in_thread,
        thread_has_previous_answer,
        originator_in_cc
    ]

    return wide_columns, []


def build_estimator(model_dir, model_type):
    """Build an estimator appropriate for the given model type."""
    wide_columns, deep_columns = build_model_columns()
    hidden_units = [100, 75, 50, 25]

    # Create a tf.estimator.RunConfig to ensure the model is run on CPU, which
    # trains faster than GPU for this model.
    run_config = tf.estimator.RunConfig().replace(
        session_config=tf.ConfigProto(device_count={'GPU': 0}))

    if model_type == 'wide':
        return tf.estimator.LinearClassifier(
            model_dir=model_dir,
            feature_columns=wide_columns,
            config=run_config)
    elif model_type == 'deep':
        return tf.estimator.DNNClassifier(
            model_dir=model_dir,
            feature_columns=deep_columns,
            hidden_units=hidden_units,
            config=run_config)
    else:
        return tf.estimator.DNNLinearCombinedClassifier(
            model_dir=model_dir,
            linear_feature_columns=wide_columns,
            dnn_feature_columns=deep_columns,
            dnn_hidden_units=hidden_units,
            config=run_config)


features_spec = {
    'messageId': tf.FixedLenFeature([], tf.string),
    'threadId': tf.FixedLenFeature([], tf.string),
    'subject': tf.FixedLenFeature([], tf.string),
    'body_word_count': tf.FixedLenFeature([], tf.int64),
    'subject_word_count': tf.FixedLenFeature([], tf.int64),
    'originator_in_cc': tf.FixedLenFeature([], tf.int64),
    'amount_of_participants': tf.FixedLenFeature([], tf.int64),
    'no_in_thread': tf.FixedLenFeature([], tf.int64),
    'first_in_thread': tf.FixedLenFeature([], tf.int64),
    'thread_has_previous_answer': tf.FixedLenFeature([], tf.int64),
    'contact_title': tf.FixedLenFeature([], tf.string),
    'body': tf.FixedLenFeature([], tf.string),
    'answered': tf.FixedLenFeature([], tf.int64)}


def input_fn(data_file, num_epochs, shuffle, batch_size):
    """Generate an input function for the Estimator."""

    def parse(value):
        print('Parsing', data_file)
        features = tf.parse_single_example(value, features=features_spec)
        features['answered'] = tf.Print(features['answered'], [features['answered']])
        return features, features.pop('answered')  # tf.equal(features.pop('answered'), 'true')

    dataset = tf.data.TFRecordDataset(data_file)
    dataset = dataset.map(parse, 1)
    dataset = dataset.shuffle(100)
    dataset = dataset.batch(batch_size)
    return dataset


def main(unused_argv):
    # Clean up the model directory if present
    shutil.rmtree(FLAGS.model_dir, ignore_errors=True)
    model = build_estimator(FLAGS.model_dir, FLAGS.model_type)
    files = [FLAGS.train_data + "part-{:05d}-of-{:05d}.tfrecords".format(i, 10) for i in range(10)]
    # Train and evaluate the model every `FLAGS.epochs_per_eval` epochs.
    for n in range(FLAGS.train_epochs // FLAGS.epochs_per_eval):
        shuffle(files)

        model.train(input_fn=lambda: input_fn(
            files, FLAGS.epochs_per_eval, True, FLAGS.batch_size))

        results = model.evaluate(input_fn=lambda: input_fn(
            files, 1, False, FLAGS.batch_size))

        # Display evaluation metrics
        print('Results at epoch', (n + 1) * FLAGS.epochs_per_eval)
        print('-' * 60)

        for key in sorted(results):
            print('%s: %s' % (key, results[key]))

    def serving_input_receiver_fn():
        """An input receiver that expects a serialized tf.Example."""
        serialized_tf_example = tf.placeholder(dtype=tf.string,
                                               name='input_example_tensor')
        receiver_tensors = {'examples': serialized_tf_example}
        features = tf.parse_example(serialized_tf_example, features_spec)
        return tf.estimator.export.ServingInputReceiver(features, receiver_tensors)

    model.export_savedmodel("/Users/oleksiimo/projects/public/data/saved_model", serving_input_receiver_fn)


if __name__ == '__main__':
    tf.logging.set_verbosity(tf.logging.INFO)
    FLAGS, unparsed = parser.parse_known_args()
    tf.app.run(main=main, argv=[sys.argv[0]] + unparsed)
