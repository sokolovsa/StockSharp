using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Windows;
using System.Windows.Threading;
using Ecng.Backup;
using Ecng.Backup.Yandex;
using Ecng.Collections;
using Ecng.Common;
using Ecng.Configuration;
using Ecng.Xaml;
using Ecng.Xaml.Charting.Common;
using MoreLinq;
using StockSharp.Algo;
using StockSharp.Algo.Candles;
using StockSharp.Algo.Indicators;
using StockSharp.Algo.Storages;
using StockSharp.Logging;
using StockSharp.Xaml.Charting.IndicatorPainters;
using StockSharp.BusinessEntities;
using StockSharp.Configuration;
using StockSharp.Localization;
using StockSharp.Messages;
using StockSharp.Xaml.Charting;

namespace SampleChartMultithreadingStress
{
	public partial class MainWindow
	{
		class Logger : BaseLogReceiver {}

		public static readonly DependencyProperty IsTestRunningProperty = DependencyProperty.Register("IsTestRunning", typeof(bool), typeof(MainWindow), new PropertyMetadata(default(bool)));

		public bool IsTestRunning { get => (bool) GetValue(IsTestRunningProperty); set => SetValue(IsTestRunningProperty, value); }

		const int MaxAdditionalAreas = 5;

		static readonly TimeSpan _timeframe = TimeSpan.FromMinutes(1);

		readonly Random _rnd = new Random(Environment.TickCount);

		readonly Logger _log = new Logger();
		readonly LogManager _logManager = new LogManager();
		readonly List<ChartArea> _areas = new List<ChartArea>();
		ChartArea _mainArea;

		private ChartCandleElement _candleElement;
		private TimeFrameCandle _candle;
		private CandleMessageVolumeProfile _volumeProfile;
		private readonly DispatcherTimer _uiTimer = new DispatcherTimer();
		private Security _security;
		private readonly CachedSynchronizedDictionary<ChartIndicatorElement, IIndicator> _indicators = new CachedSynchronizedDictionary<ChartIndicatorElement, IIndicator>();

		readonly ManualResetEvent _dataThreadStoppedEvt = new ManualResetEvent(true);
		bool _stopDataThread;
		Exception _error;

		readonly SynchronizedList<Action> _dataThreadActions = new SynchronizedList<Action>();
		readonly CachedSynchronizedList<TimeFrameCandle> _allCandles = new CachedSynchronizedList<TimeFrameCandle>();

		public MainWindow()
		{
			InitializeComponent();

			_logManager.FlushInterval = TimeSpan.FromMilliseconds(10);
			_logManager.Sources.Add(_log);
			_logManager.Listeners.Add(new FileLogListener { FileName = "sample_log" });

			Title = Title.Put(LocalizedStrings.Str3200);

			Loaded += OnLoaded;

			PreviewMouseDoubleClick += (sender, args) => { Chart.IsAutoRange = true; };
			PreviewMouseWheel += (sender, args) => { Chart.IsAutoRange = false; };
			PreviewMouseRightButtonDown += (sender, args) => { Chart.IsAutoRange = false; };

			_uiTimer.Interval = TimeSpan.FromMilliseconds(100);
			_uiTimer.Tick += OnUITimerTick;
			_uiTimer.Start();
		}

		private void OnLoaded(object sender, RoutedEventArgs routedEventArgs)
		{
			Chart.FillIndicators();
			Chart.SubscribeIndicatorElement += Chart_OnSubscribeIndicatorElement;
			Chart.UnSubscribeElement += Chart_OnUnSubscribeElement;

			ConfigManager.RegisterService<IBackupService>(new YandexDiskService());
		}

		private void Chart_OnSubscribeIndicatorElement(ChartIndicatorElement element, CandleSeries series, IIndicator indicator)
		{
			PostToDataThread(() =>
			{
				var chartData = new ChartDrawData();

				foreach (var candle in _allCandles.Cache)
					chartData.Group(candle.OpenTime).Add(element, indicator.Process(candle));

				Chart.Reset(new [] {element});
				Chart.Draw(chartData);

				_indicators[element] = indicator;
			});
		}

		private void Chart_OnUnSubscribeElement(IChartElement element)
		{
			if (element is ChartIndicatorElement indElem)
				_indicators.Remove(indElem);
		}

		void AddArea()
		{
			ChartArea area;
			_areas.Add(area = new ChartArea());
			Chart.AddArea(area);

			var indicator = new AverageDirectionalIndex();
			var el = new ChartIndicatorElement
			{
				DrawStyle = ChartIndicatorDrawStyles.Line,
				YAxisId = area.YAxises.First().Id,
				IndicatorPainter = new AverageDirectionalIndexPainter()
			};

			Chart.AddElement(area, el, (CandleSeries) Chart.GetSource(_candleElement), indicator);
		}

		void RemoveRandomArea()
		{
			var idx = _rnd.Next(1, Chart.Areas.Count - 1);
			Chart.RemoveArea(Chart.Areas[idx]);
		}

		void Reset()
		{
			StopDataThread();

			Chart.ClearAreas();
			_areas.Clear();
			_dataThreadActions.Clear();
			_indicators.Clear();

			_areas.Add(_mainArea = new ChartArea());
			Chart.AddArea(_mainArea);

			Chart.IsAutoRange = true;
			Chart.IsAutoScroll = true;

			_mainArea.YAxises.First().AutoRange = true;

			var id = new SecurityIdGenerator().Split("RIZ2@FORTS");

			_security = new Security
			{
				Id = id.ToStringId(),
				PriceStep = 10,
				Board = ExchangeBoard.Associated
			};

			var series = new CandleSeries(typeof(TimeFrameCandle), _security, _timeframe) { IsCalcVolumeProfile = true };
			_candleElement = new ChartCandleElement { FullTitle = "Candles" };
			Chart.AddElement(_mainArea, _candleElement, series);

			_candle = null;
			_allCandles.Clear();

			for (var i = 0; i < MaxAdditionalAreas; ++i)
				AddArea();

			Chart.Reset(new IChartElement[] { _candleElement });
		}

		private void DoTest()
		{
			Reset();
			StartDataThread();
		}

		private void StartStopClick(object sender, RoutedEventArgs e)
		{
			if (!IsTestRunning)
			{
				IsTestRunning = true;
				DoTest();
			}
			else
			{
				StopDataThread();
			}
		}

		private void OnUITimerTick(object sender, EventArgs eventArgs)
		{
			if(ChkAddRemoveAreas.IsChecked != true || !IsTestRunning)
				return;

			var remove = Chart.Areas.Count > 1 && (Chart.Areas.Count >= MaxAdditionalAreas + 1 || _rnd.NextDouble() < 0.1);

			_log.AddInfoLog(remove ? "remove area" : "add area");

			if(remove)
				RemoveRandomArea();
			else
				AddArea();
		}

		void AppendTick(Security security, ExecutionMessage tick)
		{
			var time = tick.ServerTime;
			var price = tick.TradePrice.Value;
			var dd = new ChartDrawData();
			var append = false;

			if (_candle == null || time >= _candle.CloseTime)
			{
				if (_candle != null)
				{
					_candle.State = CandleStates.Finished;
					var g = dd.Group(_candle.OpenTime);

					g.Add(_candleElement, _candle);
					foreach(var pair in _indicators.CachedPairs)
						g.Add(pair.Key, pair.Value.Process(_candle));
				}

				var bounds = _timeframe.GetCandleBounds(time, security.Board);

				append = true;
				_candle = new TimeFrameCandle
				{
					TimeFrame = _timeframe,
					OpenTime = bounds.Min,
					CloseTime = bounds.Max,
					Security = security,
				};

				_volumeProfile = new CandleMessageVolumeProfile();
				_candle.PriceLevels = _volumeProfile.PriceLevels;

				_candle.OpenPrice = _candle.HighPrice = _candle.LowPrice = _candle.ClosePrice = price;
			}

			if (time < _candle.OpenTime)
				throw new InvalidOperationException("invalid time");

			if (price > _candle.HighPrice)
				_candle.HighPrice = price;

			if (price < _candle.LowPrice)
				_candle.LowPrice = price;

			_candle.ClosePrice = price;

			_candle.TotalVolume += tick.TradeVolume ?? 0;

			if(append)
				_allCandles.Add(_candle);

			_volumeProfile.Update(tick.TradePrice.Value, tick.TradeVolume, tick.OriginSide);

			var g1 = dd.Group(_candle.OpenTime);
			g1.Add(_candleElement, _candle);
			foreach(var pair in _indicators.CachedPairs)
				g1.Add(pair.Key, pair.Value.Process(_candle));

			Chart.Draw(dd);
		}

		void DataThreadProc()
		{
			var maxDays = 30;
			var path = @"..\..\..\..\Testing\HistoryData\".ToFullPath();
			var storage = new StorageRegistry();
			var date = DateTime.MinValue;

			foreach (var tick in storage.GetTickMessageStorage(_security, new LocalMarketDataDrive(path)).Load())
			{
				if(_stopDataThread)
					break;

				DoDataThreadActions();

				AppendTick(_security, tick);

				if (date != tick.ServerTime.Date)
				{
					date = tick.ServerTime.Date;

					maxDays--;
					if (maxDays == 0)
						break;
				}
			}
		}

		void StartDataThread()
		{
			_dataThreadStoppedEvt.WaitOne();
			_dataThreadStoppedEvt.Reset();
			_stopDataThread = false;
			_error = null;

			ThreadingHelper.Thread(() =>
			{
				try
				{
					DataThreadProc();
				}
				catch (Exception e)
				{
					_error = e;
					_log.AddErrorLog($"Data thread error:\n{e}");
				}
				finally
				{
					_dataThreadStoppedEvt.Set();
					this.GuiAsync(() =>
					{
						_dataThreadActions.Clear();
						IsTestRunning = false;
						_error.Do(e => ShowError(e.ToString()));
						_error = null;
					});
				}
			}).Launch();
		}

		void StopDataThread()
		{
			_stopDataThread = true;
			_dataThreadStoppedEvt.WaitOne();
		}

		void PostToDataThread(Action action) => _dataThreadActions.Add(action);

		void DoDataThreadActions()
		{
			if(!_dataThreadActions.IsEmpty())
				_dataThreadActions.SyncDo(l => l.CopyAndClear().ForEach(a => a()));
		}

		void ShowError(string msg)
		{
			new MessageBoxBuilder()
				.Owner(this)
				.Error()
				.Text(msg)
				.Show();
		}
	}
}