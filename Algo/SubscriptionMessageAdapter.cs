namespace StockSharp.Algo
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Ecng.Collections;
	using Ecng.Common;

	using StockSharp.Localization;
	using StockSharp.Messages;

	/// <summary>
	/// Subscription counter adapter.
	/// </summary>
	public class SubscriptionMessageAdapter : MessageAdapterWrapper
	{
		private sealed class SubscriptionInfo
		{
			public MarketDataMessage Message { get; }

			public IList<MarketDataMessage> Subscriptions { get; }

			public int Subscribers { get; set; }

			public bool IsSubscribed { get; set; }

			public SubscriptionInfo(MarketDataMessage message)
			{
				if (message == null)
					throw new ArgumentNullException(nameof(message));

				Message = message;
				Subscriptions = new List<MarketDataMessage>();
			}
		}

		private readonly SyncObject _sync = new SyncObject();

		private readonly Dictionary<Tuple<MarketDataTypes, SecurityId, object, DateTimeOffset?, DateTimeOffset?, long?, int?>, SubscriptionInfo> _subscribers = new Dictionary<Tuple<MarketDataTypes, SecurityId, object, DateTimeOffset?, DateTimeOffset?, long?, int?>, SubscriptionInfo>();
		private readonly Dictionary<Tuple<MarketDataTypes, SecurityId, object>, SubscriptionInfo> _candleSubscribers = new Dictionary<Tuple<MarketDataTypes, SecurityId, object>, SubscriptionInfo>();
		private readonly Dictionary<string, SubscriptionInfo> _newsSubscribers = new Dictionary<string, SubscriptionInfo>(StringComparer.InvariantCultureIgnoreCase);
		private readonly Dictionary<string, RefPair<PortfolioMessage, int>> _pfSubscribers = new Dictionary<string, RefPair<PortfolioMessage, int>>(StringComparer.InvariantCultureIgnoreCase);
		//private readonly Dictionary<Tuple<MarketDataTypes, SecurityId>, List<MarketDataMessage>> _pendingMessages = new Dictionary<Tuple<MarketDataTypes, SecurityId>, List<MarketDataMessage>>();

		/// <summary>
		/// Initializes a new instance of the <see cref="SubscriptionMessageAdapter"/>.
		/// </summary>
		/// <param name="innerAdapter">Inner message adapter.</param>
		public SubscriptionMessageAdapter(IMessageAdapter innerAdapter)
			: base(innerAdapter)
		{
		}

		/// <summary>
		/// Restore subscription on reconnect.
		/// </summary>
		public bool IsRestoreOnReconnect { get; set; }

		private void ClearSubscribers()
		{
			_subscribers.Clear();
			_newsSubscribers.Clear();
			_pfSubscribers.Clear();
			_candleSubscribers.Clear();
		}

		/// <summary>
		/// Send message.
		/// </summary>
		/// <param name="message">Message.</param>
		public override void SendInMessage(Message message)
		{
			if (message.IsBack)
			{
				if (message.Adapter == this)
				{
					message.Adapter = null;
					message.IsBack = false;
				}
				else
				{
					base.SendInMessage(message);
					return;
				}
			}

			switch (message.Type)
			{
				case MessageTypes.Reset:
				{
					lock (_sync)
					{
						ClearSubscribers();
						//_pendingMessages.Clear();
					}

					base.SendInMessage(message);
					break;
				}

				case MessageTypes.Disconnect:
				{
					if (!IsRestoreOnReconnect)
					{
						var messages = new List<Message>();

						lock (_sync)
						{
							if (_newsSubscribers.Count > 0)
								messages.AddRange(_newsSubscribers.Values.Select(p => p.Message));

							if (_subscribers.Count > 0)
								messages.AddRange(_subscribers.Values.Select(p => p.Message));

							if (_candleSubscribers.Count > 0)
								messages.AddRange(_candleSubscribers.Values.Select(p => p.Message));

							if (_pfSubscribers.Count > 0)
								messages.AddRange(_pfSubscribers.Values.Select(p => p.First));
						
							ClearSubscribers();
						}

						foreach (var m in messages)
						{
							var msg = m.Clone();

							if (msg is MarketDataMessage mdMsg)
							{
								mdMsg.TransactionId = TransactionIdGenerator.GetNextId();
								mdMsg.IsSubscribe = false;
							}
							else
							{
								var pfMsg = (PortfolioMessage)msg;

								pfMsg.TransactionId = TransactionIdGenerator.GetNextId();
								pfMsg.IsSubscribe = false;
							}

							base.SendInMessage(msg);

						}
					}

					base.SendInMessage(message);
					break;
				}

				case MessageTypes.MarketData:
					ProcessInMarketDataMessage((MarketDataMessage)message);
					break;

				case MessageTypes.Portfolio:
					ProcessInPortfolioMessage((PortfolioMessage)message);
					break;

				default:
					base.SendInMessage(message);
					break;
			}
		}

		/// <summary>
		/// Process <see cref="MessageAdapterWrapper.InnerAdapter"/> output message.
		/// </summary>
		/// <param name="message">The message.</param>
		protected override void OnInnerAdapterNewOutMessage(Message message)
		{
			if (message.IsBack)
			{
				base.OnInnerAdapterNewOutMessage(message);
				return;
			}

			List<Message> messages = null;

			switch (message.Type)
			{
				case MessageTypes.Connect:
				{
					var connectMsg = (ConnectMessage)message;

					if (connectMsg.Error == null && IsRestoreOnReconnect)
					{
						messages = new List<Message>();

						lock (_sync)
						{
							messages.AddRange(_subscribers.Values.Select(p => p.Message));
							messages.AddRange(_newsSubscribers.Values.Select(p => p.Message));
							messages.AddRange(_candleSubscribers.Values.Select(p => p.Message));
							messages.AddRange(_pfSubscribers.Values.Select(p => p.First));

							ClearSubscribers();
						}

						if (messages.Count == 0)
							messages = null;
					}

					break;
				}

				case MessageTypes.MarketData:
				{
					if (ProcessOutMarketDataMessage((MarketDataMessage)message))
						return;
					
					break;
				}
			}

			base.OnInnerAdapterNewOutMessage(message);

			if (messages != null)
			{
				foreach (var m in messages)
				{
					var msg = m.Clone();

					msg.IsBack = true;
					msg.Adapter = this;

					if (msg is MarketDataMessage mdMsg)
					{
						mdMsg.TransactionId = TransactionIdGenerator.GetNextId();
					}
					else
					{
						var pfMsg = (PortfolioMessage)msg;
						pfMsg.TransactionId = TransactionIdGenerator.GetNextId();
					}

					base.OnInnerAdapterNewOutMessage(msg);
				}
			}
		}

		private void ProcessInMarketDataMessage(MarketDataMessage message)
		{
			var sendIn = false;
			MarketDataMessage sendOutMsg = null;
			SubscriptionInfo info;
			var secIdKey = IsSupportSubscriptionBySecurity ? message.SecurityId : default(SecurityId);

			lock (_sync)
			{
				switch (message.DataType)
				{
					case MarketDataTypes.News:
					{
						var key = message.NewsId ?? string.Empty;
						info = ProcessSubscription(_newsSubscribers, key, message, ref sendIn, ref sendOutMsg);
						break;
					}
					case MarketDataTypes.CandleTimeFrame:
					case MarketDataTypes.CandleRange:
					case MarketDataTypes.CandlePnF:
					case MarketDataTypes.CandleRenko:
					case MarketDataTypes.CandleTick:
					case MarketDataTypes.CandleVolume:
					{
						var key = Tuple.Create(message.DataType, secIdKey, message.Arg);
						info = ProcessSubscription(_candleSubscribers, key, message, ref sendIn, ref sendOutMsg);
						break;
					}
					default:
					{
						var key = message.CreateKey(secIdKey);
						info = ProcessSubscription(_subscribers, key, message, ref sendIn, ref sendOutMsg);
						break;
					}
				}
			}

			if (sendIn)
			{
				if (!message.IsSubscribe && message.OriginalTransactionId == 0)
					message.OriginalTransactionId = info.Message.TransactionId;

				base.SendInMessage(message);
			}

			if (sendOutMsg != null)
				RaiseNewOutMessage(sendOutMsg);
		}

		private bool ProcessOutMarketDataMessage(MarketDataMessage message)
		{
			var secIdKey = IsSupportSubscriptionBySecurity ? message.SecurityId : default(SecurityId);

			lock (_sync)
			{
				switch (message.DataType)
				{
					case MarketDataTypes.News:
					{
						var key = message.NewsId ?? string.Empty;
						return ProcessSubscriptionResult(_newsSubscribers, key, message);
					}
					case MarketDataTypes.CandleTimeFrame:
					case MarketDataTypes.CandleRange:
					case MarketDataTypes.CandlePnF:
					case MarketDataTypes.CandleRenko:
					case MarketDataTypes.CandleTick:
					case MarketDataTypes.CandleVolume:
					{
						var key = Tuple.Create(message.DataType, secIdKey, message.Arg);
						return ProcessSubscriptionResult(_candleSubscribers, key, message);
					}
					default:
					{
						var key = message.CreateKey(secIdKey);
						return ProcessSubscriptionResult(_subscribers, key, message);
					}
				}
			}
		}

		private bool ProcessSubscriptionResult<T>(Dictionary<T, SubscriptionInfo> subscriptions, T key, MarketDataMessage message)
		{
			var info = subscriptions.TryGetValue(key);

			if (info == null)
				return false;

			var isSubscribe = message.IsSubscribe;
			var removeInfo = !isSubscribe || message.Error != null;

			info.IsSubscribed = isSubscribe && message.Error == null;

			foreach (var subscription in info.Subscriptions)
			{
				var reply = (MarketDataMessage)subscription.Clone();
				reply.OriginalTransactionId = subscription.TransactionId;
				//reply.TransactionId = message.TransactionId;
				reply.Error = message.Error;

				base.OnInnerAdapterNewOutMessage(reply);
			}

			if (removeInfo)
				subscriptions.Remove(key);

			return true;
		}

		private static SubscriptionInfo ProcessSubscription<T>(Dictionary<T, SubscriptionInfo> subscriptions, T key, MarketDataMessage message, ref bool sendIn, ref MarketDataMessage sendOutMsg)
		{
			MarketDataMessage clone = null;
			var info = subscriptions.TryGetValue(key) ?? new SubscriptionInfo(clone = (MarketDataMessage)message.Clone());
			var subscribersCount = info.Subscribers;
			var isSubscribe = message.IsSubscribe;

			if (isSubscribe)
			{
				subscribersCount++;
				sendIn = subscribersCount == 1;
			}
			else
			{
				if (subscribersCount > 0)
				{
					subscribersCount--;
					sendIn = subscribersCount == 0;
				}
				else
					sendOutMsg = NonExist(message);
			}

			if (sendOutMsg != null)
				return info;

			//if (isSubscribe)
			info.Subscriptions.Add(clone ?? (MarketDataMessage)message.Clone());

			if (!sendIn && info.IsSubscribed)
			{
				sendOutMsg = new MarketDataMessage
				{
					DataType = message.DataType,
					IsSubscribe = isSubscribe,
					SecurityId = message.SecurityId,
					Arg = message.Arg,
					OriginalTransactionId = message.TransactionId,
				};
			}

			if (subscribersCount > 0)
			{
				info.Subscribers = subscribersCount;
				subscriptions[key] = info;
			}
			else
				subscriptions.Remove(key);

			return info;
		}

		private static MarketDataMessage NonExist(MarketDataMessage message)
		{
			return new MarketDataMessage
			{
				DataType = message.DataType,
				IsSubscribe = false,
				SecurityId = message.SecurityId,
				OriginalTransactionId = message.TransactionId,
				Error = new InvalidOperationException(LocalizedStrings.SubscriptionNonExist),
			};
		}

		private void ProcessInPortfolioMessage(PortfolioMessage message)
		{
			var sendIn = false;
			var pfName = message.PortfolioName;
			
			RefPair<PortfolioMessage, int> pair;

			lock (_sync)
			{
				pair = _pfSubscribers.TryGetValue(pfName) ?? RefTuple.Create((PortfolioMessage)message.Clone(), 0);
				var subscribersCount = pair.Second;

				if (message.IsSubscribe)
				{
					subscribersCount++;
					sendIn = subscribersCount == 1;
				}
				else
				{
					if (subscribersCount > 0)
					{
						subscribersCount--;
						sendIn = subscribersCount == 0;
					}
					//else
					//	sendOutMsg = NonExist(message);
				}

				if (subscribersCount > 0)
				{
					pair.Second = subscribersCount;
					_pfSubscribers[pfName] = pair;
				}
				else
					_pfSubscribers.Remove(pfName);
			}

			if (sendIn)
			{
				if (!message.IsSubscribe && message.OriginalTransactionId == 0)
					message.OriginalTransactionId = pair.First.TransactionId;

				base.SendInMessage(message);
			}
		}

		/// <summary>
		/// Create a copy of <see cref="SubscriptionMessageAdapter"/>.
		/// </summary>
		/// <returns>Copy.</returns>
		public override IMessageChannel Clone()
		{
			return new SubscriptionMessageAdapter(InnerAdapter);
		}
	}
}