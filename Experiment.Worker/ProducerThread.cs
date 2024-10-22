using System.Collections.Concurrent;
using Experiment.Common;

namespace Experiment.Worker;

internal static class ProducerThread
{
    public static void Run(
        int numEpoch,
        int numDetConsumer, 
        int numNonDetConsumer, 
        int detBufferSize, 
        int nonDetBufferSize,
        bool[] isEpochFinish,
        bool[] isProducerFinish,
        Dictionary<int, Queue<(bool, RequestData)>> shared_requests,
        Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>> thread_requests)
    {
        Console.WriteLine($"Start ProducerThread, detBufferSize = {detBufferSize}, nonDetBufferSize = {nonDetBufferSize}");
        for (int eIndex = 0; eIndex < numEpoch; eIndex++)
        {
            var producer_queue = shared_requests[eIndex];
            while (producer_queue.Count > 0 && !isEpochFinish[eIndex])
            {
                var txn = producer_queue.Dequeue();
                var isDet = txn.Item1;
                var isConsumed = false;
                if (isDet)     // keep checking detThread until the txn is put to the consumer buffer
                {
                    while (!isConsumed && !isEpochFinish[eIndex])
                    {
                        for (int detThread = 0; detThread < numDetConsumer; detThread++)
                        {
                            if (thread_requests[eIndex][detThread].Count < detBufferSize)
                            {
                                thread_requests[eIndex][detThread].Enqueue(txn.Item2);
                                isConsumed = true;
                                break;
                            }
                        }
                        if (isConsumed) break;
                    }
                }
                else   // keep checking nonDet thread until the txn is consumed by the consumerThread
                {
                    while (!isConsumed && !isEpochFinish[eIndex])
                    {
                        for (int nonDetThread = numDetConsumer; nonDetThread < numDetConsumer + numNonDetConsumer; nonDetThread++)
                        {
                            if (thread_requests[eIndex][nonDetThread].Count < nonDetBufferSize)
                            {
                                thread_requests[eIndex][nonDetThread].Enqueue(txn.Item2);
                                isConsumed = true;
                                break;
                            }
                        }
                        if (isConsumed) break;
                    }
                }
            }

            isProducerFinish[eIndex] = true;   // when Count == 0, set true
            shared_requests.Remove(eIndex);
        }
    }
}
