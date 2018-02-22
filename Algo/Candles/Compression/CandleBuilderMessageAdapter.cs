namespace StockSharp.Algo.Candles.Compression
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Ecng.Collections;
	using Ecng.Common;

	using StockSharp.Algo.Storages;
	using StockSharp.BusinessEntities;
	using StockSharp.Localization;
	using StockSharp.Messages;

	/// <summary>
	/// Candle builder adapter.
	/// </summary>
	public class CandleBuilderMessageAdapter : MessageAdapterWrapper
	{
		private readonly IExchangeInfoProvider _exchangeInfoProvider;

		private sealed class CandleBuildersList : SynchronizedList<ICandleBuilder>
		{
			private readonly Dictionary<MarketDataTypes, ICandleBuilder> _builders = new Dictionary<MarketDataTypes, ICandleBuilder>();

			public ICandleBuilder Get(MarketDataTypes type)
			{
				return _builders.TryGetValue(type);
			}

			protected override void OnAdded(ICandleBuilder item)
			{
				_builders.Add(item.CandleType, item);
				base.OnAdded(item);
			}

			protected override bool OnRemoving(ICandleBuilder item)
			{
				_builders.RemoveWhere(p => p.Value == item);
				return base.OnRemoving(item);
			}

			protected override void OnInserted(int index, ICandleBuilder item)
			{
				_builders.Add(item.CandleType, item);
				base.OnInserted(index, item);
			}

			protected override bool OnClearing()
			{
				_builders.Clear();
				return base.OnClearing();
			}
		}

		private sealed class SeriesInfo
		{
			public MarketDataMessage MarketDataMessage { get; set; }

			public ICandleBuilderValueTransform Transform { get; set; }

			public DateTimeOffset? LastTime { get; set; }

			public long TransactionId { get; set; }

			public bool IsHistory { get; set; }

			public Tuple<DateTimeOffset, WorkingTimePeriod> CurrentPeriod { get; set; }

			public ExchangeBoard Board { get; set; }

			public CandleMessage CurrentCandleMessage { get; set; }

			private MarketDataTypes[] _supportedMarketDataTypes = ArrayHelper.Empty<MarketDataTypes>();

			public MarketDataTypes[] SupportedMarketDataTypes
			{
				get => _supportedMarketDataTypes;
				set
				{
					if (value == null)
						throw new ArgumentNullException(nameof(value));

					_supportedMarketDataTypes = value;
				}
			}
		}

		private class DummyCandleBuilderValueTransform : BaseCandleBuilderValueTransform
		{
			public DummyCandleBuilderValueTransform(MarketDataTypes buildFrom)
				: base(buildFrom)
			{
			}
		}

		private readonly Dictionary<SecurityId, List<SeriesInfo>> _seriesInfos = new Dictionary<SecurityId, List<SeriesInfo>>();
		private readonly Dictionary<long, SeriesInfo> _seriesInfosByTransactions = new Dictionary<long, SeriesInfo>();
		private readonly OrderedPriorityQueue<DateTimeOffset, SeriesInfo> _seriesInfosByDates = new OrderedPriorityQueue<DateTimeOffset, SeriesInfo>();
		private readonly Dictionary<Tuple<SecurityId, MarketDataTypes, object>, List<SeriesInfo>> _series = new Dictionary<Tuple<SecurityId, MarketDataTypes, object>, List<SeriesInfo>>();
		private readonly SyncObject _sync = new SyncObject();

		private readonly CandleBuildersList _candleBuilders;

		/// <summary>
		/// Candles builders.
		/// </summary>
		public IList<ICandleBuilder> Builders => _candleBuilders;

		/// <summary>
		/// Initializes a new instance of the <see cref="CandleBuilderMessageAdapter"/>.
		/// </summary>
		/// <param name="innerAdapter">Inner message adapter.</param>
		/// <param name="exchangeInfoProvider">The exchange boards provider.</param>
		public CandleBuilderMessageAdapter(IMessageAdapter innerAdapter, IExchangeInfoProvider exchangeInfoProvider)
			: base(innerAdapter)
		{
			if (exchangeInfoProvider == null)
				throw new ArgumentNullException(nameof(exchangeInfoProvider));

			_exchangeInfoProvider = exchangeInfoProvider;

			_candleBuilders = new CandleBuildersList
			{
				new TimeFrameCandleBuilder(exchangeInfoProvider),
				new TickCandleBuilder(),
				new VolumeCandleBuilder(),
				new RangeCandleBuilder(),
				new RenkoCandleBuilder(),
				new PnFCandleBuilder(),
			};
		}

		/// <summary>
		/// Send message.
		/// </summary>
		/// <param name="message">Message.</param>
		public override void SendInMessage(Message message)
		{
			if (message == null)
				throw new ArgumentNullException(nameof(message));

			switch (message.Type)
			{
				case MessageTypes.MarketData:
				{
					var mdMsg = (MarketDataMessage)message;

					switch (mdMsg.DataType)
					{
						case MarketDataTypes.CandleTimeFrame:
						case MarketDataTypes.CandleTick:
						case MarketDataTypes.CandleVolume:
						case MarketDataTypes.CandleRange:
						case MarketDataTypes.CandlePnF:
						case MarketDataTypes.CandleRenko:
							ProcessMarketDataMessage(mdMsg);
							break;

						default:
						{
							if (message.IsBack && message.Adapter == this)
								message.IsBack = false;

							base.SendInMessage(message);
							break;
						}
					}

					break;
				}

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

			switch (message.Type)
			{
				case MessageTypes.MarketData:
				{
					ProcessMarketDataResponse((MarketDataMessage)message);
					break;
				}

				case MessageTypes.MarketDataFinished:
				{
					ProcessMarketDataFinished((MarketDataFinishedMessage)message);
					break;
				}

				case MessageTypes.Time:
				{
					base.OnInnerAdapterNewOutMessage(message);
					ProcessTime();
					break;
				}

				case MessageTypes.Execution:
				{
					base.OnInnerAdapterNewOutMessage(message);
					
					var execMsg = (ExecutionMessage)message;

					if (execMsg.ExecutionType != ExecutionTypes.Tick && execMsg.ExecutionType != ExecutionTypes.OrderLog)
						break;

					ProcessValue(execMsg.SecurityId, execMsg.OriginalTransactionId, execMsg);
					break;
				}

				case MessageTypes.QuoteChange:
				{
					base.OnInnerAdapterNewOutMessage(message);

					var quoteMsg = (QuoteChangeMessage)message;

					ProcessValue(quoteMsg.SecurityId, 0, quoteMsg);
					break;
				}

				case MessageTypes.Level1Change:
				{
					base.OnInnerAdapterNewOutMessage(message);

					var l1Msg = (Level1ChangeMessage)message;
					
					ProcessValue(l1Msg.SecurityId, 0, l1Msg);
					break;
				}

				case MessageTypes.CandleTimeFrame:
				case MessageTypes.CandlePnF:
				case MessageTypes.CandleRange:
				case MessageTypes.CandleRenko:
				case MessageTypes.CandleTick:
				case MessageTypes.CandleVolume:
				{
					ProcessCandle((CandleMessage)message);
					break;
				}

				default:
					base.OnInnerAdapterNewOutMessage(message);
					break;
			}
		}

		private void ProcessMarketDataMessage(MarketDataMessage msg)
		{
			var securityId = msg.SecurityId;

			if (msg.IsSubscribe)
			{
				if (msg.IsBack)
				{
					if (_seriesInfosByTransactions.ContainsKey(msg.TransactionId))
					{
						base.SendInMessage(msg);
						return;
					}
				}

				var info = new SeriesInfo
				{
					MarketDataMessage = (MarketDataMessage)msg.Clone(),
					LastTime = msg.From,
					Board = !securityId.BoardCode.IsEmpty() ? _exchangeInfoProvider.GetOrCreateBoard(securityId.BoardCode) : ExchangeBoard.Associated
				};

				_seriesInfos
					.SafeAdd(securityId)
					.Add(info);

				_seriesInfosByDates
					.Add(new KeyValuePair<DateTimeOffset, SeriesInfo>(msg.To ?? DateTimeOffset.MaxValue, info));

				_series
					.SafeAdd(Tuple.Create(securityId, msg.DataType, msg.Arg))
					.Add(info);

				Subscribe(info, false);
			}
			else
			{
				var subscriptions = _seriesInfos.TryGetValue(securityId);

				if (subscriptions == null)
					return;

				var removed = subscriptions.RemoveWhere(s => s.MarketDataMessage.TransactionId == msg.OriginalTransactionId);

				foreach (var info in removed)
					UnSubscribe(info, false);
			}
		}

		private void ProcessTime()
		{
			return;
			// TODO check for calls from different threads
			lock (_sync)
			{
				if (_seriesInfosByDates.Count == 0)
					return;

				var pair = _seriesInfosByDates.Peek();

				while (pair.Key <= CurrentTime)
				{
					_seriesInfosByDates.Dequeue();

					UnSubscribe(pair.Value, true);
					SendMarketDataFinished(pair.Value);

					if (_seriesInfosByDates.Count == 0)
						break;

					pair = _seriesInfosByDates.Peek();
				}
			}
		}

		private void ProcessMarketDataResponse(MarketDataMessage mdMsg)
		{
			RaiseNewOutMessage(mdMsg);

			var info = _seriesInfosByTransactions.TryGetValue(mdMsg.OriginalTransactionId);

			if (info == null)
				return;

			SetAvailableMarketDataType(info, mdMsg);

			if (!mdMsg.IsNotSupported && mdMsg.Error == null)
			{
				info.IsHistory = mdMsg.IsHistory;

				RaiseNewOutMessage(new MarketDataMessage { OriginalTransactionId = info.MarketDataMessage.TransactionId });
				return;
			}

			_seriesInfosByTransactions.Remove(mdMsg.OriginalTransactionId);

			switch (info.Transform.BuildFrom)
			{
				case MarketDataTypes.Level1:
				case MarketDataTypes.MarketDepth:
				case MarketDataTypes.Trades:
				case MarketDataTypes.OrderLog:
				{
					SendNotSupported(info);
					break;
				}

				case MarketDataTypes.CandleTimeFrame:
				case MarketDataTypes.CandleTick:
				case MarketDataTypes.CandleVolume:
				case MarketDataTypes.CandleRange:
				case MarketDataTypes.CandlePnF:
				case MarketDataTypes.CandleRenko:
				{
					if (info.MarketDataMessage.BuildCandlesMode != BuildCandlesModes.LoadAndBuild)
					{
						SendNotSupported(info);
					}
					else
						Subscribe(info, true);

					break;
				}
			}
		}

		private void ProcessMarketDataFinished(MarketDataFinishedMessage message)
		{
			var info = _seriesInfosByTransactions.TryGetValue(message.OriginalTransactionId);

			if (info == null)
			{
				RaiseNewOutMessage(message);
				return;
			}

			if (message.IsHistory)
			{
				if (!info.MarketDataMessage.IsHistory)
					return;
				
				//RemoveSeriesInfo(info);
				//RaiseNewOutMessage(new MarketDataFinishedMessage { OriginalTransactionId = info.MarketDataMessage.TransactionId, IsHistory = true });

				//return;
			}

			SetAvailableMarketDataType(info, message);

			_seriesInfosByTransactions.Remove(message.OriginalTransactionId);

			switch (info.Transform.BuildFrom)
			{
				case MarketDataTypes.Level1:
				case MarketDataTypes.MarketDepth:
				case MarketDataTypes.Trades:
				case MarketDataTypes.OrderLog:
				{
					SendMarketDataFinished(info);
					break;
				}

				case MarketDataTypes.CandleTimeFrame:
				case MarketDataTypes.CandleTick:
				case MarketDataTypes.CandleVolume:
				case MarketDataTypes.CandleRange:
				case MarketDataTypes.CandlePnF:
				case MarketDataTypes.CandleRenko:
				{
					if (info.MarketDataMessage.BuildCandlesMode != BuildCandlesModes.LoadAndBuild)
					{
						SendMarketDataFinished(info);
					}
					else
						Subscribe(info, true);

					break;
				}
			}
		}

		private void Subscribe(SeriesInfo info, bool isBack)
		{
			info.TransactionId = TransactionIdGenerator.GetNextId();
			info.Transform = GetCurrentDataType(info);

			var msg = (MarketDataMessage)info.MarketDataMessage.Clone();
			msg.TransactionId = info.TransactionId;

			if (!isBack && !msg.IsRealTimeSubscription())
			{
				msg.From = info.LastTime;

				if (msg.To != null && msg.From >= msg.To)
					return;
			}

			var reseted = ResetMarketDataMessageArg(info, msg);

			_seriesInfosByTransactions.Add(info.TransactionId, info);

			msg.ValidateBounds();

			if (isBack || reseted)
			{
				msg.IsBack = true;
				msg.Adapter = this;
				RaiseNewOutMessage(msg);
			}
			else
				base.SendInMessage(msg);
		}

		private void UnSubscribe(SeriesInfo info, bool isBack)
		{
			SendMarketDataFinished(info);

			if (info.Transform == null)
				return;

			var mdMsg = (MarketDataMessage)info.MarketDataMessage.Clone();
			mdMsg.OriginalTransactionId = info.TransactionId;
			mdMsg.IsSubscribe = false;

			var reseted = ResetMarketDataMessageArg(info, mdMsg);

			if (isBack || reseted)
			{
				mdMsg.IsBack = true;
				RaiseNewOutMessage(mdMsg);
			}
			else
				base.SendInMessage(mdMsg);
		}

		private static bool ResetMarketDataMessageArg(SeriesInfo info, MarketDataMessage msg)
		{
			var buildFrom = info.Transform.BuildFrom;

			if (msg.DataType == buildFrom)
				return false;

			if (buildFrom.IsCandleDataType())
				throw new InvalidOperationException(buildFrom.ToString());

			msg.DataType = buildFrom;
			msg.Arg = null;

			return true;
		}

		private static ICandleBuilderValueTransform GetCurrentDataType(SeriesInfo info)
		{
			var dataType = info.MarketDataMessage.DataType;

			switch (info.MarketDataMessage.BuildCandlesMode)
			{
				case BuildCandlesModes.LoadAndBuild:
					return info.Transform == null ? CreateTransform(dataType, null) : CreateTransform(info);

				case BuildCandlesModes.Load:
					return CreateTransform(dataType, null);

				case BuildCandlesModes.Build:
					return CreateTransform(info);

				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		private static ICandleBuilderValueTransform CreateTransform(SeriesInfo info)
		{
			if (info.MarketDataMessage.BuildCandlesFrom != null)
				return CreateTransform(info.MarketDataMessage.BuildCandlesFrom.Value, info.MarketDataMessage.BuildCandlesField);

			if (info.SupportedMarketDataTypes.Contains(MarketDataTypes.Trades))
				return CreateTransform(MarketDataTypes.Trades, null);

			if (info.SupportedMarketDataTypes.Contains(MarketDataTypes.MarketDepth))
				return CreateTransform(MarketDataTypes.MarketDepth, null);

			if (info.SupportedMarketDataTypes.Contains(MarketDataTypes.Level1))
				return CreateTransform(MarketDataTypes.Level1, null);

			return CreateTransform(MarketDataTypes.Trades, null);
		}

		private static ICandleBuilderValueTransform CreateTransform(MarketDataTypes dataType, Level1Fields? field)
		{
			switch (dataType)
			{
				case MarketDataTypes.Trades:
					return new TickCandleBuilderValueTransform();

				case MarketDataTypes.MarketDepth:
				{
					var t = new QuoteCandleBuilderValueTransform();

					if (field != null)
						t.Type = field.Value;

					return t;
				}

				case MarketDataTypes.Level1:
				{
					var t = new Level1CandleBuilderValueTransform();

					if (field != null)
						t.Type = field.Value;

					return t;
				}

				case MarketDataTypes.OrderLog:
				{
					var t = new OrderLogCandleBuilderValueTransform();

					if (field != null)
						t.Type = field.Value;

					return t;
				}

				case MarketDataTypes.CandleTimeFrame:
				case MarketDataTypes.CandleTick:
				case MarketDataTypes.CandleVolume:
				case MarketDataTypes.CandleRange:
				case MarketDataTypes.CandlePnF:
				case MarketDataTypes.CandleRenko:
					return new DummyCandleBuilderValueTransform(dataType);

				default:
					throw new ArgumentOutOfRangeException(nameof(dataType), dataType, LocalizedStrings.Str1219);
			}
		}

		private static void SetAvailableMarketDataType(SeriesInfo info, Message msg)
		{
			if (info.SupportedMarketDataTypes.IsEmpty() && msg.Adapter != null)
				info.SupportedMarketDataTypes = msg.Adapter.SupportedMarketDataTypes;
		}

		private void SendNotSupported(SeriesInfo info)
		{
			RemoveSeriesInfo(info);

			var msg = new MarketDataMessage
			{
				OriginalTransactionId = info.MarketDataMessage.TransactionId,
				IsNotSupported = true
			};

			RaiseNewOutMessage(msg);
		}

		private void SendMarketDataFinished(SeriesInfo info)
		{
			RemoveSeriesInfo(info);

			var msg = new MarketDataFinishedMessage
			{
				OriginalTransactionId = info.MarketDataMessage.TransactionId
			};

			RaiseNewOutMessage(msg);
		}

		private void SendCandle(SeriesInfo info, CandleMessage candleMsg)
		{
			//if (info.LastTime > candleMsg.OpenTime)
			//	return;

			info.LastTime = candleMsg.OpenTime;

			var clone = (CandleMessage)candleMsg.Clone();
			clone.Adapter = candleMsg.Adapter;
			clone.OriginalTransactionId = info.MarketDataMessage.TransactionId;

			RaiseNewOutMessage(clone);
		}

		private void RemoveSeriesInfo(SeriesInfo info)
		{
			_seriesInfos
				.SafeAdd(info.MarketDataMessage.SecurityId)
				.RemoveWhere(i => i == info);

			_seriesInfosByTransactions.Remove(info.TransactionId);

			_seriesInfosByDates.RemoveWhere(p => p.Value == info);

			_series.Remove(Tuple.Create(info.MarketDataMessage.SecurityId, info.MarketDataMessage.DataType, info.MarketDataMessage.Arg));
		}

		private void ProcessCandle(CandleMessage candleMsg)
		{
			var info = _seriesInfosByTransactions.TryGetValue(candleMsg.OriginalTransactionId);

			if (info != null)
			{
				SendCandle(info, candleMsg);

				if (!info.IsHistory)
					TryProcessCandles(candleMsg, info);
			}
			else
			{
				//RaiseNewOutMessage(candleMsg);
				TryProcessCandles(candleMsg, null);
			}
		}

		private void TryProcessCandles(CandleMessage candleMsg, SeriesInfo info)
		{
			var key = Tuple.Create(candleMsg.SecurityId, candleMsg.Type.ToCandleMarketDataType(), candleMsg.Arg);
			var infos = _series.TryGetValue(key);

			if (infos == null)
				return;

			foreach (var seriesInfo in infos)
			{
				if (seriesInfo == info)
					continue;

				if (seriesInfo.IsHistory)
					continue;

				if (seriesInfo.LastTime > candleMsg.OpenTime)
					continue;

				SendCandle(seriesInfo, candleMsg);
			}
		}

		private void ProcessValue<TMessage>(SecurityId securityId, long transactionId, TMessage message)
			where TMessage : Message
		{
			var infos = _seriesInfos.TryGetValue(securityId);

			if (infos == null)
				return;

			foreach (var info in infos)
			{
				var transform = info.Transform;

				if (transform?.Process(message) != true)
					continue;

				if (info.TransactionId != transactionId && (transactionId != 0 || info.IsHistory))
					continue;

				if (!CheckTime(info, transform.Time))
					continue;

				var mdMsg = info.MarketDataMessage;
				var builder = _candleBuilders.Get(mdMsg.DataType);

				if (builder == null)
					throw new InvalidOperationException($"Builder for {mdMsg.DataType} not found.");

				info.LastTime = transform.Time;

				var result = builder.Process(mdMsg, info.CurrentCandleMessage, transform);

				foreach (var candleMessage in result)
				{
					info.CurrentCandleMessage = candleMessage;
					SendCandle(info, candleMessage);
				}
			}
		}

		//private static bool CheckTime(SeriesInfo info, DateTimeOffset time)
		//{
		//	if (info.LastTime > time)
		//		return false;

		//	if (!((info.MarketDataMessage.From == null || time >= info.MarketDataMessage.From) && (info.MarketDataMessage.To == null || time < info.MarketDataMessage.To)))
		//		return false;

		//	if (info.CurrentPeriod == null || info.CurrentPeriod.Item1.Date.Date != time.Date.Date)
		//		return CheckTime(info, time);

		//	var exchangeTime = time.ToLocalTime(info.Board.TimeZone);
		//	var tod = exchangeTime.TimeOfDay;
		//	var period = info.CurrentPeriod.Item2;

		//	var res = period == null || period.Times.IsEmpty() || period.Times.Any(r => r.Contains(tod));

		//	if (res)
		//		return true;

		//	return CheckTime(info, time);
		//}

		private static bool CheckTime(SeriesInfo info, DateTimeOffset time)
		{
			if (info.LastTime > time)
				return false;

			var res = info.Board.IsTradeTime(time, out var period);
			info.CurrentPeriod = Tuple.Create(time, period);

			return res;
		}

		/// <summary>
		/// Create a copy of <see cref="CandleBuilderMessageAdapter"/>.
		/// </summary>
		/// <returns>Copy.</returns>
		public override IMessageChannel Clone()
		{
			return new CandleBuilderMessageAdapter(InnerAdapter, _exchangeInfoProvider);
		}
	}
}