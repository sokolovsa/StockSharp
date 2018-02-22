#region S# License
/******************************************************************************************
NOTICE!!!  This program and source code is owned and licensed by
StockSharp, LLC, www.stocksharp.com
Viewing or use of this code requires your acceptance of the license
agreement found at https://github.com/StockSharp/StockSharp/blob/master/LICENSE
Removal of this comment is a violation of the license agreement.

Project: StockSharp.Algo.Algo
File: BasketMessageAdapter.cs
Created: 2015, 11, 11, 2:32 PM

Copyright 2010 by StockSharp, LLC
*******************************************************************************************/
#endregion S# License
namespace StockSharp.Algo
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Ecng.Collections;
	using Ecng.Common;
	using Ecng.Serialization;

	using MoreLinq;

	using StockSharp.Algo.Candles.Compression;
	using StockSharp.Algo.Storages;
	using StockSharp.Logging;
	using StockSharp.Messages;
	using StockSharp.Localization;
	using SubscriptionInfo = System.Tuple<Messages.MarketDataTypes, Messages.SecurityId, object, System.DateTimeOffset?, System.DateTimeOffset?, long?, int?>;

	/// <summary>
	/// The interface describing the list of adapters to trading systems with which the aggregator operates.
	/// </summary>
	public interface IInnerAdapterList : ISynchronizedCollection<IMessageAdapter>, INotifyList<IMessageAdapter>
	{
		/// <summary>
		/// Internal adapters sorted by operation speed.
		/// </summary>
		IEnumerable<IMessageAdapter> SortedAdapters { get; }

		/// <summary>
		/// The indexer through which speed priorities (the smaller the value, then adapter is faster) for internal adapters are set.
		/// </summary>
		/// <param name="adapter">The internal adapter.</param>
		/// <returns>The adapter priority. If the -1 value is set the adapter is considered to be off.</returns>
		int this[IMessageAdapter adapter] { get; set; }
	}

	/// <summary>
	/// Adapter-aggregator that allows simultaneously to operate multiple adapters connected to different trading systems.
	/// </summary>
	public class BasketMessageAdapter : MessageAdapter
	{
		private sealed class InnerAdapterList : CachedSynchronizedList<IMessageAdapter>, IInnerAdapterList
		{
			private readonly Dictionary<IMessageAdapter, int> _enables = new Dictionary<IMessageAdapter, int>();

			public IEnumerable<IMessageAdapter> SortedAdapters
			{
				get { return Cache.Where(t => this[t] != -1).OrderBy(t => this[t]); }
			}

			protected override bool OnAdding(IMessageAdapter item)
			{
				_enables.Add(item, 0);
				return base.OnAdding(item);
			}

			protected override bool OnInserting(int index, IMessageAdapter item)
			{
				_enables.Add(item, 0);
				return base.OnInserting(index, item);
			}

			protected override bool OnRemoving(IMessageAdapter item)
			{
				_enables.Remove(item);
				return base.OnRemoving(item);
			}

			protected override bool OnClearing()
			{
				_enables.Clear();
				return base.OnClearing();
			}

			public int this[IMessageAdapter adapter]
			{
				get
				{
					lock (SyncRoot)
						return _enables.TryGetValue2(adapter) ?? -1;
				}
				set
				{
					if (value < -1)
						throw new ArgumentOutOfRangeException();

					lock (SyncRoot)
					{
						if (!Contains(adapter))
							Add(adapter);

						_enables[adapter] = value;
						//_portfolioTraders.Clear();
					}
				}
			}
		}

		private readonly SynchronizedDictionary<long, MarketDataMessage> _subscriptionMessages = new SynchronizedDictionary<long, MarketDataMessage>();
		private readonly SynchronizedDictionary<long, IMessageAdapter> _subscriptionsById = new SynchronizedDictionary<long, IMessageAdapter>();
		private readonly Dictionary<long, HashSet<IMessageAdapter>> _subscriptionNonSupportedAdapters = new Dictionary<long, HashSet<IMessageAdapter>>();
		private readonly SynchronizedDictionary<SubscriptionInfo, IMessageAdapter> _subscriptionsByKey = new SynchronizedDictionary<SubscriptionInfo, IMessageAdapter>();
		private readonly SynchronizedDictionary<IMessageAdapter, HeartbeatMessageAdapter> _hearbeatAdapters = new SynchronizedDictionary<IMessageAdapter, HeartbeatMessageAdapter>();
		private readonly SyncObject _connectedResponseLock = new SyncObject();
		private readonly Dictionary<MessageTypes, CachedSynchronizedSet<IMessageAdapter>> _messageTypeAdapters = new Dictionary<MessageTypes, CachedSynchronizedSet<IMessageAdapter>>();
		private readonly HashSet<IMessageAdapter> _pendingConnectAdapters = new HashSet<IMessageAdapter>();
		private readonly Queue<Message> _pendingMessages = new Queue<Message>();
		private readonly HashSet<HeartbeatMessageAdapter> _connectedAdapters = new HashSet<HeartbeatMessageAdapter>();
		private bool _isFirstConnect;
		private readonly InnerAdapterList _innerAdapters;

		/// <summary>
		/// Adapters with which the aggregator operates.
		/// </summary>
		public IInnerAdapterList InnerAdapters => _innerAdapters;

		private INativeIdStorage _nativeIdStorage = new InMemoryNativeIdStorage();

		/// <summary>
		/// Security native identifier storage.
		/// </summary>
		public INativeIdStorage NativeIdStorage
		{
			get => _nativeIdStorage;
			set
			{
				if (value == null)
					throw new ArgumentNullException(nameof(value));

				_nativeIdStorage = value;
			}
		}

		private ISecurityMappingStorage _securityMappingStorage;

		/// <summary>
		/// Security identifier mappings storage.
		/// </summary>
		public ISecurityMappingStorage SecurityMappingStorage
		{
			get => _securityMappingStorage;
			set
			{
				if (value == null)
					throw new ArgumentNullException(nameof(value));

				_securityMappingStorage = value;
			}
		}

		/// <summary>
		/// Extended info <see cref="Message.ExtensionInfo"/> storage.
		/// </summary>
		public IExtendedInfoStorage ExtendedInfoStorage { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="BasketMessageAdapter"/>.
		/// </summary>
		/// <param name="transactionIdGenerator">Transaction id generator.</param>
		public BasketMessageAdapter(IdGenerator transactionIdGenerator)
			: this(transactionIdGenerator, new InMemoryMessageAdapterProvider())
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="BasketMessageAdapter"/>.
		/// </summary>
		/// <param name="transactionIdGenerator">Transaction id generator.</param>
		/// <param name="adapterProvider">The message adapter's provider.</param>
		public BasketMessageAdapter(IdGenerator transactionIdGenerator, IPortfolioMessageAdapterProvider adapterProvider)
			: base(transactionIdGenerator)
		{
			if (adapterProvider == null)
				throw new ArgumentNullException(nameof(adapterProvider));

			_innerAdapters = new InnerAdapterList();
			AdapterProvider = adapterProvider;
		}

		/// <summary>
		/// The message adapter's provider.
		/// </summary>
		public IPortfolioMessageAdapterProvider AdapterProvider { get; }

		/// <summary>
		/// Supported by adapter message types.
		/// </summary>
		public override MessageTypes[] SupportedMessages
		{
			get { return GetSortedAdapters().SelectMany(a => a.SupportedMessages).Distinct().ToArray(); }
		}

		/// <summary>
		/// <see cref="PortfolioLookupMessage"/> required to get portfolios and positions.
		/// </summary>
		public override bool PortfolioLookupRequired
		{
			get { return GetSortedAdapters().Any(a => a.PortfolioLookupRequired); }
		}

		/// <summary>
		/// <see cref="OrderStatusMessage"/> required to get orders and own trades.
		/// </summary>
		public override bool OrderStatusRequired
		{
			get { return GetSortedAdapters().Any(a => a.OrderStatusRequired); }
		}

		/// <summary>
		/// <see cref="SecurityLookupMessage"/> required to get securities.
		/// </summary>
		public override bool SecurityLookupRequired
		{
			get { return GetSortedAdapters().Any(a => a.SecurityLookupRequired); }
		}

		/// <summary>
		/// Gets a value indicating whether the connector supports position lookup.
		/// </summary>
		protected override bool IsSupportNativePortfolioLookup => true;

		/// <summary>
		/// Gets a value indicating whether the connector supports security lookup.
		/// </summary>
		protected override bool IsSupportNativeSecurityLookup => true;

		/// <summary>
		/// Restore subscription on reconnect.
		/// </summary>
		public bool IsRestorSubscriptioneOnReconnect { get; set; }

		/// <inheritdoc />
		public override IEnumerable<TimeSpan> TimeFrames
		{
			get { return GetSortedAdapters().SelectMany(a => a.TimeFrames); }
		}

		/// <summary>
		/// Create condition for order type <see cref="OrderTypes.Conditional"/>, that supports the adapter.
		/// </summary>
		/// <returns>Order condition. If the connection does not support the order type <see cref="OrderTypes.Conditional"/>, it will be returned <see langword="null" />.</returns>
		public override OrderCondition CreateOrderCondition()
		{
			throw new NotSupportedException();
		}

		/// <summary>
		/// Check the connection is alive. Uses only for connected states.
		/// </summary>
		/// <returns><see langword="true" />, is the connection still alive, <see langword="false" />, if the connection was rejected.</returns>
		public override bool IsConnectionAlive()
		{
			throw new NotSupportedException();
		}

		private void ProcessReset(Message message)
		{
			_hearbeatAdapters.Values.ForEach(a =>
			{
				a.SendInMessage(message);
				a.Dispose();
			});

			lock (_connectedResponseLock)
			{
				_connectedAdapters.Clear();
				_messageTypeAdapters.Clear();
				_pendingConnectAdapters.Clear();
				_pendingMessages.Clear();
				_subscriptionNonSupportedAdapters.Clear();
			}

			_hearbeatAdapters.Clear();
			_subscriptionsById.Clear();
			_subscriptionsByKey.Clear();
			_subscriptionMessages.Clear();
		}

		private IMessageAdapter CreateWrappers(IMessageAdapter adapter)
		{
			if (adapter.IsFullCandlesOnly)
			{
				adapter = new CandleHolderMessageAdapter(adapter);
			}

			if (adapter.IsNativeIdentifiers)
			{
				adapter = new SecurityNativeIdMessageAdapter(adapter, NativeIdStorage);
			}

			if (SecurityMappingStorage != null && !adapter.StorageName.IsEmpty())
			{
				adapter = new SecurityMappingMessageAdapter(adapter, SecurityMappingStorage);
			}

			if (ExtendedInfoStorage != null && !adapter.SecurityExtendedFields.IsEmpty())
			{
				adapter = new ExtendedInfoStorageMessageAdapter(adapter, ExtendedInfoStorage, adapter.StorageName, adapter.SecurityExtendedFields);
			}

			if (adapter.IsSupportSubscriptions)
			{
				adapter = new SubscriptionMessageAdapter(adapter) { IsRestoreOnReconnect = IsRestorSubscriptioneOnReconnect };
			}

			return adapter;
		}

		/// <summary>
		/// Send message.
		/// </summary>
		/// <param name="message">Message.</param>
		protected override void OnSendInMessage(Message message)
		{
			if (message.IsBack)
			{
				var adapter = message.Adapter;

				if (adapter == null)
					throw new InvalidOperationException();

				if (adapter != this)
				{
					adapter.SendInMessage(message);
					return;	
				}
			}

			switch (message.Type)
			{
				case MessageTypes.Reset:
					ProcessReset(message);
					break;

				case MessageTypes.Connect:
				{
					if (_isFirstConnect)
						_isFirstConnect = false;
					else
						ProcessReset(new ResetMessage());

					_hearbeatAdapters.AddRange(GetSortedAdapters().ToDictionary(a => a, a =>
					{
						lock (_connectedResponseLock)
							_pendingConnectAdapters.Add(a);

						var wrapper = CreateWrappers(a);
						var hearbeatAdapter = new HeartbeatMessageAdapter(wrapper);
						((IMessageAdapter)hearbeatAdapter).Parent = this;
						hearbeatAdapter.NewOutMessage += m => OnInnerAdapterNewOutMessage(wrapper, m);
						return hearbeatAdapter;
					}));
					
					if (_hearbeatAdapters.Count == 0)
						throw new InvalidOperationException(LocalizedStrings.Str3650);

					_hearbeatAdapters.Values.ForEach(a => a.SendInMessage(message));
					break;
				}

				case MessageTypes.Disconnect:
				{
					lock (_connectedResponseLock)
						_connectedAdapters.ForEach(a => a.SendInMessage(message));

					break;
				}

				case MessageTypes.Portfolio:
				{
					var pfMsg = (PortfolioMessage)message;
					ProcessPortfolioMessage(pfMsg.PortfolioName, pfMsg);
					break;
				}

				case MessageTypes.OrderRegister:
				case MessageTypes.OrderReplace:
				case MessageTypes.OrderCancel:
				case MessageTypes.OrderGroupCancel:
				{
					var ordMsg = (OrderMessage)message;
					ProcessAdapterMessage(ordMsg.PortfolioName, ordMsg);
					break;
				}

				case MessageTypes.OrderPairReplace:
				{
					var ordMsg = (OrderPairReplaceMessage)message;
					ProcessAdapterMessage(ordMsg.Message1.PortfolioName, ordMsg);
					break;
				}

				case MessageTypes.MarketData:
				{
					ProcessMarketDataRequest((MarketDataMessage)message);
					break;
				}

				case MessageTypes.ChangePassword:
				{
					var adapter = GetSortedAdapters().FirstOrDefault(a => a.SupportedMessages.Contains(MessageTypes.ChangePassword));

					if (adapter == null)
						throw new InvalidOperationException(LocalizedStrings.Str629Params.Put(message.Type));

					adapter.SendInMessage(message);
					break;
				}

				default:
				{
					ProcessOtherMessage(message);
					break;
				}
			}
		}

		private void ProcessOtherMessage(Message message)
		{
			if (message.Adapter != null)
			{
				message.Adapter.SendInMessage(message);
				return;
			}

			GetAdapters(message)?.ForEach(a => a.SendInMessage(message));
		}

		private IMessageAdapter[] GetAdapters(Message message)
		{
			IMessageAdapter[] adapters;

			lock (_connectedResponseLock)
			{
				adapters = _messageTypeAdapters.TryGetValue(message.Type)?.Cache;

				if (adapters != null && message.Type == MessageTypes.MarketData)
				{
					var set = _subscriptionNonSupportedAdapters.TryGetValue(((MarketDataMessage)message).TransactionId);

					if (set != null)
					{
						adapters = adapters.Where(a => !set.Contains(GetUnderlyingAdapter(a))).ToArray();

						if (adapters.Length == 0)
							adapters = null;
					}
				}

				if (adapters == null)
				{
					if (_pendingConnectAdapters.Count > 0)
					{
						_pendingMessages.Enqueue(message.Clone());
						return null;
					}
				}
			}

			if (adapters == null || adapters.Length == 0)
				throw new InvalidOperationException(LocalizedStrings.Str629Params.Put(message));

			return adapters;
		}

		private IMessageAdapter GetSubscriptionAdapter(MarketDataMessage mdMsg)
		{
			if (mdMsg.Adapter != null)
			{
				var wrapper = _hearbeatAdapters.TryGetValue(mdMsg.Adapter);

				if (wrapper != null)
					return wrapper;
			}

			return GetAdapters(mdMsg)?.First();
		}

		private void ProcessMarketDataRequest(MarketDataMessage mdMsg)
		{
			switch (mdMsg.DataType)
			{
				case MarketDataTypes.News:
				{
					var adapter = GetSubscriptionAdapter(mdMsg);
					adapter?.SendInMessage(mdMsg);
					break;
				}

				default:
				{
					var key = mdMsg.CreateKey();

					if (mdMsg.TransactionId == 0)
						throw new InvalidOperationException("TransId == 0");

					var adapter = mdMsg.IsSubscribe
							? GetSubscriptionAdapter(mdMsg)
							: (_subscriptionsById.TryGetValue(mdMsg.OriginalTransactionId) ?? _subscriptionsByKey.TryGetValue(key));

					if (adapter == null)
						break;

					// if the message was looped back via IsBack=true
					_subscriptionMessages.TryAdd(mdMsg.TransactionId, (MarketDataMessage)mdMsg.Clone());
					adapter.SendInMessage(mdMsg);

					break;
				}
			}
		}

		private void ProcessAdapterMessage(string portfolioName, Message message)
		{
			var adapter = message.Adapter;

			if (adapter == null)
				ProcessPortfolioMessage(portfolioName, message);
			else
				adapter.SendInMessage(message);
		}

		private void ProcessPortfolioMessage(string portfolioName, Message message)
		{
			var adapter = portfolioName.IsEmpty() ? null : AdapterProvider.GetAdapter(portfolioName);

			if (adapter == null)
			{
				var adapters = GetAdapters(message);

				if (adapters == null)
					return;

				adapter = adapters.First();
			}
			else
			{
				var a = _hearbeatAdapters.TryGetValue(adapter);

				if (a == null)
					throw new InvalidOperationException(LocalizedStrings.Str1838Params.Put(adapter.GetType()));

				adapter = a;
			}

			adapter.SendInMessage(message);
		}

		/// <summary>
		/// The embedded adapter event <see cref="IMessageChannel.NewOutMessage"/> handler.
		/// </summary>
		/// <param name="innerAdapter">The embedded adapter.</param>
		/// <param name="message">Message.</param>
		protected virtual void OnInnerAdapterNewOutMessage(IMessageAdapter innerAdapter, Message message)
		{
			if (!message.IsBack)
			{
				if (message.Adapter == null)
					message.Adapter = innerAdapter;

				switch (message.Type)
				{
					case MessageTypes.Connect:
						ProcessConnectMessage(innerAdapter, (ConnectMessage)message);
						return;

					case MessageTypes.Disconnect:
						ProcessDisconnectMessage(innerAdapter, (DisconnectMessage)message);
						return;

					case MessageTypes.MarketData:
						ProcessMarketDataResponse(innerAdapter, (MarketDataMessage)message);
						return;

					case MessageTypes.Portfolio:
						var pfMsg = (PortfolioMessage)message;
						AdapterProvider.SetAdapter(pfMsg.PortfolioName, GetUnderlyingAdapter(innerAdapter));
						break;

					case MessageTypes.PortfolioChange:
						var pfChangeMsg = (PortfolioChangeMessage)message;
						AdapterProvider.SetAdapter(pfChangeMsg.PortfolioName, GetUnderlyingAdapter(innerAdapter));
						break;

					//case MessageTypes.Position:
					//	var posMsg = (PositionMessage)message;
					//	AdapterProvider.SetAdapter(posMsg.PortfolioName, GetUnderlyingAdapter(innerAdapter));
					//	break;

					case MessageTypes.PositionChange:
						var posChangeMsg = (PositionChangeMessage)message;
						AdapterProvider.SetAdapter(posChangeMsg.PortfolioName, GetUnderlyingAdapter(innerAdapter));
						break;
				}
			}

			SendOutMessage(message);
		}

		private static IMessageAdapter GetUnderlyingAdapter(IMessageAdapter adapter)
		{
			return adapter is IMessageAdapterWrapper wrapper ? GetUnderlyingAdapter(wrapper.InnerAdapter) : adapter;
		}

		private void ProcessConnectMessage(IMessageAdapter innerAdapter, ConnectMessage message)
		{
			var underlyingAdapter = GetUnderlyingAdapter(innerAdapter);
			var heartbeatAdapter = _hearbeatAdapters[underlyingAdapter];

			var isError = message.Error != null;

			Message[] pendingMessages;

			if (isError)
				this.AddErrorLog(LocalizedStrings.Str625Params, underlyingAdapter.GetType().Name, message.Error);

			lock (_connectedResponseLock)
			{
				_pendingConnectAdapters.Remove(underlyingAdapter);

				if (isError)
				{
					_connectedAdapters.Remove(heartbeatAdapter);

					if (_pendingConnectAdapters.Count == 0)
					{
						pendingMessages = _pendingMessages.ToArray();
						_pendingMessages.Clear();
					}
					else
						pendingMessages = ArrayHelper.Empty<Message>();
				}
				else
				{
					foreach (var supportedMessage in innerAdapter.SupportedMessages)
					{
						_messageTypeAdapters.SafeAdd(supportedMessage).Add(heartbeatAdapter);
					}

					_connectedAdapters.Add(heartbeatAdapter);

					pendingMessages = _pendingMessages.ToArray();
					_pendingMessages.Clear();
				}
			}

			message.Adapter = underlyingAdapter;
			SendOutMessage(message);

			foreach (var pendingMessage in pendingMessages)
			{
				if (isError)
					SendOutError(LocalizedStrings.Str629Params.Put(pendingMessage.Type));
				else
				{
					pendingMessage.Adapter = this;
					pendingMessage.IsBack = true;
					SendOutMessage(pendingMessage);
				}
			}
		}

		private void ProcessDisconnectMessage(IMessageAdapter innerAdapter, DisconnectMessage message)
		{
			var underlyingAdapter = GetUnderlyingAdapter(innerAdapter);
			var heartbeatAdapter = _hearbeatAdapters[underlyingAdapter];

			if (message.Error != null)
				this.AddErrorLog(LocalizedStrings.Str627Params, underlyingAdapter.GetType().Name, message.Error);

			lock (_connectedResponseLock)
			{
				foreach (var supportedMessage in innerAdapter.SupportedMessages)
				{
					var list = _messageTypeAdapters.TryGetValue(supportedMessage);

					list.Remove(heartbeatAdapter);

					if (list.Count == 0)
						_messageTypeAdapters.Remove(supportedMessage);
				}

				_connectedAdapters.Add(heartbeatAdapter);
			}

			message.Adapter = underlyingAdapter;
			SendOutMessage(message);
		}

		private void ProcessMarketDataResponse(IMessageAdapter adapter, MarketDataMessage message)
		{
			var originalTransactionId = message.OriginalTransactionId;
			var originMsg = _subscriptionMessages.TryGetValue(originalTransactionId);

			if (originMsg == null)
			{
				SendOutMessage(message);
				return;
			}

			var key = originMsg.CreateKey();

			var error = message.Error;

			var isSubscribe = originMsg.IsSubscribe;

			if (message.IsNotSupported)
			{
				lock (_connectedResponseLock)
				{
					var set = _subscriptionNonSupportedAdapters.SafeAdd(originalTransactionId, k => new HashSet<IMessageAdapter>());
					set.Add(GetUnderlyingAdapter(adapter));

					originMsg.Adapter = this;
					originMsg.IsBack = true;
					SendOutMessage(originMsg);
				}

				return;
			}
			
			if (error == null && isSubscribe)
			{
				_subscriptionsByKey.Add(key, adapter);
				_subscriptionsById.Add(originalTransactionId, adapter);
			}

			RaiseMarketDataMessage(adapter, originalTransactionId, error, isSubscribe);
		}

		private void RaiseMarketDataMessage(IMessageAdapter adapter, long originalTransactionId, Exception error, bool isSubscribe)
		{
			SendOutMessage(new MarketDataMessage
			{
				OriginalTransactionId = originalTransactionId,
				Error = error,
				Adapter = adapter,
				IsSubscribe = isSubscribe,
			});
		}

		/// <summary>
		/// To get adapters <see cref="IInnerAdapterList.SortedAdapters"/> sorted by the specified priority. By default, there is no sorting.
		/// </summary>
		/// <returns>Sorted adapters.</returns>
		protected IEnumerable<IMessageAdapter> GetSortedAdapters()
		{
			return _innerAdapters.SortedAdapters;
		}

		/// <summary>
		/// Save settings.
		/// </summary>
		/// <param name="storage">Settings storage.</param>
		public override void Save(SettingsStorage storage)
		{
			lock (InnerAdapters.SyncRoot)
			{
				storage.SetValue(nameof(InnerAdapters), InnerAdapters.Select(a =>
				{
					var s = new SettingsStorage();

					s.SetValue("AdapterType", a.GetType().GetTypeName(false));
					s.SetValue("AdapterSettings", a.Save());
					s.SetValue("Priority", InnerAdapters[a]);

					return s;
				}).ToArray());
			}

			base.Save(storage);
		}

		/// <summary>
		/// Load settings.
		/// </summary>
		/// <param name="storage">Settings storage.</param>
		public override void Load(SettingsStorage storage)
		{
			lock (InnerAdapters.SyncRoot)
			{
				InnerAdapters.Clear();

				foreach (var s in storage.GetValue<IEnumerable<SettingsStorage>>(nameof(InnerAdapters)))
				{
					try
					{
						var adapter = s.GetValue<Type>("AdapterType").CreateInstance<IMessageAdapter>(TransactionIdGenerator);
						adapter.Load(s.GetValue<SettingsStorage>("AdapterSettings"));
						InnerAdapters[adapter] = s.GetValue<int>("Priority");
					}
					catch (Exception e)
					{
						e.LogError();
					}
				}
			}

			base.Load(storage);
		}

		/// <summary>
		/// To release allocated resources.
		/// </summary>
		protected override void DisposeManaged()
		{
			_hearbeatAdapters.Values.ForEach(a => ((IMessageAdapter)a).Parent = null);

			base.DisposeManaged();
		}
	}
}