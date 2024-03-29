﻿using System;
using System.Threading;
using System.Threading.Tasks;
using JustSaying.Messaging.MessageProcessingStrategies;
using System.Threading.Channels;

namespace JustSaying.ChannelsProcessing
{
    public enum ChannelsMessageProcessingStrategyMode
    {
        Exclusive,
        Shared
    }
    
    public sealed class ChannelsMessageProcessingStrategy : IMessageProcessingStrategy
    {
        readonly int workerCount;
        readonly int batchSize;
        readonly Channel<Func<Task>> channel;

        /// <summary>
        /// This IMessageProcessingStrategy implementation leverages Channels, it differs from the default
        /// implementation by allowing JustSaying to fetch a new batch of messages as soon after the previous
        /// have been dispatch to the channel, therefor there must be some consideration into the Channel size,
        /// the number of workers, and the batch size in order to get the best results.
        /// </summary>
        /// <param name="workerCount">
        /// The number of concurrent tasks pulling from the channel
        /// </param>
        /// <param name="batchSize">
        /// The number of messages JustSaying should request to receive from the queue(which will be capped at 10)
        /// </param>
        /// <param name="channelSize">
        /// The size of the channel, which acts as a buffer. Making this too large risks
        /// exceeding the visibility timeout for a message, which may result in double processing.
        /// </param>
        /// <param name="mode">
        /// JustSaying by default uses a single IMessageProcessingStrategy per queue, in the case where you
        /// want to share instances, use the Shared option.
        /// </param>
        public ChannelsMessageProcessingStrategy(int workerCount, int batchSize, int channelSize,
            ChannelsMessageProcessingStrategyMode mode = ChannelsMessageProcessingStrategyMode.Exclusive)
        {
            this.workerCount = workerCount;
            this.batchSize = batchSize > 10
                ? throw new ArgumentOutOfRangeException(nameof(batchSize), $"{nameof(batchSize)} should not exceed 10.")
                : batchSize;

            channel = Channel.CreateBounded<Func<Task>>(new BoundedChannelOptions(channelSize)
            {
                SingleWriter = mode == ChannelsMessageProcessingStrategyMode.Exclusive
            });

            for (var i = 0; i < workerCount; i++)
            {
                _ = Task.Run(async () =>
                {
                    while (true)
                    {
                        var action = await channel.Reader.ReadAsync();
                        await action();
                    }
                });
            }
        }

        public async Task<bool> StartWorkerAsync(Func<Task> action, CancellationToken ct)
        {
            await channel.Writer.WriteAsync(action, ct);
            return true;
        }

        // Eagerly fetch next message batch
        public Task<int> WaitForAvailableWorkerAsync() => Task.FromResult(batchSize);

        // Unused
        public int MaxConcurrency => workerCount;
    }
}
