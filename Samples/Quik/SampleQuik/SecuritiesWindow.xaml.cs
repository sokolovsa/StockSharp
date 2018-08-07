#region S# License
/******************************************************************************************
NOTICE!!!  This program and source code is owned and licensed by
StockSharp, LLC, www.stocksharp.com
Viewing or use of this code requires your acceptance of the license
agreement found at https://github.com/StockSharp/StockSharp/blob/master/LICENSE
Removal of this comment is a violation of the license agreement.

Project: SampleQuik.SampleQuikPublic
File: SecuritiesWindow.xaml.cs
Created: 2015, 11, 11, 2:32 PM

Copyright 2010 by StockSharp, LLC
*******************************************************************************************/
#endregion S# License
namespace SampleQuik
{
	using System;
	using System.Linq;
	using System.Windows;
	using System.Windows.Controls;

	using Ecng.Collections;
	using Ecng.Xaml;

	using MoreLinq;

	using StockSharp.Algo.Candles;
	using StockSharp.BusinessEntities;
	using StockSharp.Messages;
	using StockSharp.Xaml;
	using StockSharp.Localization;
	using StockSharp.Quik.Lua;

	public partial class SecuritiesWindow
	{
		private readonly SynchronizedDictionary<Security, QuotesWindow> _quotesWindows = new SynchronizedDictionary<Security, QuotesWindow>();
		private bool _initialized;

		public SecuritiesWindow()
		{
			InitializeComponent();

			CandlesPeriods.ItemsSource = LuaFixMarketDataMessageAdapter.AllTimeFrames;
			CandlesPeriods.SelectedIndex = 1;
		}

		protected override void OnClosed(EventArgs e)
		{
			_quotesWindows.SyncDo(d => d.Values.ForEach(w =>
			{
				w.DeleteHideable();
				w.Close();
			}));

			var trader = MainWindow.Instance.Trader;
			if (trader != null)
			{
				if (_initialized)
					trader.MarketDepthChanged -= TraderOnMarketDepthChanged;
			}

			base.OnClosed(e);
		}

		private void SecurityPicker_OnSecuritySelected(Security security)
		{
			NewStopOrder.IsEnabled = NewOrder.IsEnabled =
			Level1.IsEnabled = Depth.IsEnabled = security != null;

			TryEnableCandles();
		}

		private void NewOrderClick(object sender, RoutedEventArgs e)
		{
			var newOrder = new OrderWindow
			{
				Order = new Order { Security = SecurityPicker.SelectedSecurity },
				SecurityProvider = MainWindow.Instance.Trader,
				MarketDataProvider = MainWindow.Instance.Trader,
				Portfolios = new PortfolioDataSource(MainWindow.Instance.Trader),
			};

			if (newOrder.ShowModal(this))
				MainWindow.Instance.Trader.RegisterOrder(newOrder.Order);
		}

		private void NewStopOrderClick(object sender, RoutedEventArgs e)
		{
			var newOrder = new OrderConditionalWindow
			{
				Order = new Order
				{
					Security = SecurityPicker.SelectedSecurity,
					Type = OrderTypes.Conditional,
					ExpiryDate = DateTime.Today
				},
				SecurityProvider = MainWindow.Instance.Trader,
				MarketDataProvider = MainWindow.Instance.Trader,
				Portfolios = new PortfolioDataSource(MainWindow.Instance.Trader),
				Adapter = MainWindow.Instance.Trader.TransactionAdapter
			};

			if (newOrder.ShowModal(this))
				MainWindow.Instance.Trader.RegisterOrder(newOrder.Order);
		}

		private void DepthClick(object sender, RoutedEventArgs e)
		{
			var trader = MainWindow.Instance.Trader;

			foreach (var security in SecurityPicker.SelectedSecurities)
			{
				var window = _quotesWindows.SafeAdd(security, key =>
				{
					//// начинаем получать котировки стакана
					trader.RegisterMarketDepth(security);

					// создаем окно со стаканом
					var wnd = new QuotesWindow
					{
						Title = security.Id + " " + LocalizedStrings.MarketDepth
					};
					wnd.MakeHideable();
					return wnd;
				});

				if (window.Visibility == Visibility.Visible)
					window.Hide();
				else
				{
					window.Show();
					window.DepthCtrl.UpdateDepth(trader.GetMarketDepth(security));
				}

				if (!_initialized)
				{
					trader.MarketDepthChanged += TraderOnMarketDepthChanged;
					_initialized = true;

					// запросить котировки по всем стаканам сразу
					//trader.SendInMessage(new MarketDataMessage
					//{
					//	SecurityId = new SecurityId
					//	{
					//		SecurityCode = MessageAdapter.DefaultAssociatedBoardCode,
					//		BoardCode = MessageAdapter.DefaultAssociatedBoardCode
					//	},
					//	IsSubscribe = true,
					//	DataType = MarketDataTypes.MarketDepth,
					//	TransactionId = trader.TransactionIdGenerator.GetNextId()
					//});
				}
			}
		}

		private void TraderOnMarketDepthChanged(MarketDepth depth)
		{
			var wnd = _quotesWindows.TryGetValue(depth.Security);

			if (wnd != null)
				wnd.DepthCtrl.UpdateDepth(depth);
		}

		private void Level1Click(object sender, RoutedEventArgs e)
		{
			var trader = MainWindow.Instance.Trader;

			foreach (var security in SecurityPicker.SelectedSecurities)
			{
				if (trader.RegisteredSecurities.Contains(security))
				{
					trader.UnRegisterSecurity(security);
					trader.UnRegisterTrades(security);
				}
				else
				{
					trader.RegisterSecurity(security);
					trader.RegisterTrades(security);
				}
			}
		}

		private void FindClick(object sender, RoutedEventArgs e)
		{
			var wnd = new SecurityLookupWindow
			{
				ShowAllOption = MainWindow.Instance.Trader.MarketDataAdapter.IsSupportSecuritiesLookupAll,
				Criteria = new Security { Code = "SBER" }
			};

			if (!wnd.ShowModal(this))
				return;

			MainWindow.Instance.Trader.LookupSecurities(wnd.Criteria);
		}

		private void CandlesClick(object sender, RoutedEventArgs e)
		{
			foreach (var security in SecurityPicker.SelectedSecurities)
			{
				var tf = (TimeSpan)CandlesPeriods.SelectedItem;
				var series = new CandleSeries(typeof(TimeFrameCandle), security, tf);

				new ChartWindow(series, tf.Ticks == 1 ? DateTime.Today : DateTime.Now.Subtract(TimeSpan.FromTicks(tf.Ticks * 10000))).Show();
			}
		}

		private void CandlesPeriods_SelectionChanged(object sender, SelectionChangedEventArgs e)
		{
			TryEnableCandles();
		}

		private void TryEnableCandles()
		{
			Candles.IsEnabled = CandlesPeriods.SelectedItem != null && SecurityPicker.SelectedSecurity != null;
		}
	}
}