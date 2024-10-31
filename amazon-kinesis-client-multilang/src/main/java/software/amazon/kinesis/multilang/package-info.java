/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * This package provides a KCL application which implements the multi language protocol. The multi language protocol
 * defines a system for communication between a KCL multi-lang application and another process (referred to as the
 * "child process") over STDIN and STDOUT of the child process. The units of communication are JSON messages which
 * represent the actions the receiving entity should perform. The child process is responsible for reacting
 * appropriately to four different messages: initialize, processRecords, checkpoint, and shutdown. The KCL multi-lang
 * app is responsible for reacting appropriately to two messages generated by the child process: status and checkpoint.
 *
 * <h3>Action messages sent to child process</h3>
 *
 * <pre>
 * { "action"  : "initialize",
 *   "shardId" : "string",
 * }
 *
 * { "action"  : "processRecords",
 *   "records" : [{  "data" : "&lt;base64encoded_string&gt;",
 *                   "partitionKey" : "&lt;partition key&gt;",
 *                    "sequenceNumber" : "&lt;sequence number&gt";
 *               }] // a list of records
 * }
 *
 * { "action"     : "checkpoint",
 *   "checkpoint" : "&lt;sequence number&gt;",
 *   "error"      : "&lt;NameOfException&gt;"
 * }
 *
 * { "action"     : "leaseLost",
 * }
 *
 * { "action"     : "shardEnded",
 *   "checkpoint" : "&lt;SHARD_END&gt;",
 * }
 *
 * { "action"     : "shutdownRequested",
 *   "checkpoint" : "&lt;sequence number&gt;"
 * }
 * </pre>
 *
 * <h3>Action messages sent to KCL by the child process</h3>
 *
 * <pre>
 * { "action" : "checkpoint",
 *   "checkpoint" : "&lt;sequenceNumberToCheckpoint&gt";
 * }
 *
 * { "action" : "status",
 *   "responseFor" : "&lt;nameOfAction&gt";
 * }
 * </pre>
 *
 * <h3>High Level Description Of Protocol</h3>
 *
 * The child process will be started by the KCL multi-lang application. There will be one child process for each shard
 * that this worker is assigned to. The multi-lang app will send an initialize, processRecords, or shutdown message upon
 * invocation of its corresponding methods. Each message will be on a single line, the messages will be
 * separated by new lines.The child process is expected to read these messages off its STDIN line by line. The child
 * process must respond over its STDOUT with a status message indicating that is has finished performing the most recent
 * action. The multi-lang daemon will not begin to send another message until it has received the response for the
 * previous message.
 *
 * <h4>Checkpointing Behavior</h4>
 *
 * The child process may send a checkpoint message at any time <b>after</b> receiving a processRecords or shutdown
 * action and <b>before</b> sending the corresponding status message back to the processor. After sending a checkpoint
 * message over STDOUT, the child process is expected to immediately begin to read its STDIN, waiting for the checkpoint
 * result message from the KCL multi-lang processor.
 *
 * <h3>Protocol From Child Process Perspective</h3>
 *
 * <h4>Initialize</h4>
 *
 * <ol>
 * <li>Read an "initialize" action from STDIN</li>
 * <li>Perform initialization steps</li>
 * <li>Write "status" message to indicate you are done</li>
 * <li>Begin reading line from STDIN to receive next action</li>
 * </ol>
 *
 * <h4>ProcessRecords</h4>
 *
 * <ol>
 * <li>Read a "processRecords" action from STDIN</li>
 * <li>Perform processing tasks (you may write a checkpoint message at any time)</li>
 * <li>Write "status" message to STDOUT to indicate you are done.</li>
 * <li>Begin reading line from STDIN to receive next action</li>
 * </ol>
 *
 * <h4>LeaseLost</h4>
 *
 * <ol>
 * <li>Read a "leaseLost" action from STDIN</li>
 * <li>Perform lease lost tasks (you will not be able to checkpoint at this time, since the worker doesn't hold the
 * lease)</li>
 * <li>Write "status" message to STDOUT to indicate you are done.</li>
 * </ol>
 *
 * <h4>ShardEnded</h4>
 *
 * <ol>
 * <li>Read a "shardEnded" action from STDIN</li>
 * <li>Perform shutdown tasks (you should write a checkpoint message at any time)</li>
 * <li>Write "status" message to STDOUT to indicate you are done.</li>
 * </ol>
 *
 * <h4>ShutdownRequested</h4>
 *
 * <ol>
 * <li>Read a "shutdownRequested" action from STDIN</li>
 * <li>Perform shutdown requested related tasks (you may write a checkpoint message at any time)</li>
 * <li>Write "status" message to STDOUT to indicate you are done.</li>
 * </ol>
 *
 * <h4>Checkpoint</h4>
 *
 * <ol>
 * <li>Read a "checkpoint" action from STDIN</li>
 * <li>Decide whether to checkpoint again based on whether there is an error or not.</li>
 * </ol>
 *
 * <h3>Base 64 Encoding</h3>
 *
 * The "data" field of the processRecords action message is an array of arbitrary bytes. To send this in a JSON string
 * we apply base 64 encoding which transforms the byte array into a string (specifically this string doesn't have JSON
 * special symbols or new lines in it). The multi-lang processor will use the Jackson library which uses a variant of
 * MIME called MIME_NO_LINEFEEDS <a href=
 * "http://fasterxml.github.io/jackson-core/javadoc/2.3.0/com/fasterxml/jackson/core/class-use/Base64Variant.html">(see
 * Jackson doc for more details)</a> MIME is the basis of most base64 encoding variants including <a
 * href="http://tools.ietf.org/html/rfc3548.html">RFC 3548</a> which is the standard used by Python's <a
 * href="https://docs.python.org/2/library/base64.html">base64</a> module.
 *
 */
package software.amazon.kinesis.multilang;
