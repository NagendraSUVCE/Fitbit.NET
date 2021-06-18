using System;
using System.Collections.Generic;
using System.Web.Mvc;
using Fitbit.Api;
using System.Configuration;
using Fitbit.Models;
using Fitbit.Api.Portable;
using System.Threading.Tasks;
using Fitbit.Api.Portable.OAuth2;
using Newtonsoft.Json;
using System.Data;
using SampleWebMVCOAuth2.Utilities;

namespace SampleWebMVC.Controllers
{
    public class FitbitController : Controller
    {
        //static string ConnectionString = @"Server=tcp:learningtech.database.windows.net,1433;Initial Catalog=Learning;Persist Security Info=False;User ID=nagendrasubramanya;Password=AzureLearning#1;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
        static string ConnectionString = @"Server=tcp:learningsqldb.database.windows.net,1433;Initial Catalog = Learning; Persist Security Info=False;User ID = nagendra; Password=AzureCricinfo#1;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
        //
        // GET: /Fitbit/

        public ActionResult Index()
        {
            return RedirectToAction("Index", "Home");
        }

        //
        // GET: /FitbitAuth/
        // Setup - prepare the user redirect to Fitbit.com to prompt them to authorize this app.
        public ActionResult Authorize()
        {
            var appCredentials = new FitbitAppCredentials()
            {
                ClientId = ConfigurationManager.AppSettings["FitbitClientId"],
                ClientSecret = ConfigurationManager.AppSettings["FitbitClientSecret"]
            };
            //make sure you've set these up in Web.Config under <appSettings>:

            Session["AppCredentials"] = appCredentials;
            string baseurl = "https://samplewebmvcoauth220201016123544.azurewebsites.net";
            baseurl = "https://samplewebmvcoauth220210618224916.azurewebsites.net/";
            string link = $"{baseurl}Fitbit/Callback";
            //link = "http://localhost/SampleWebMVCOAuth2/Fitbit/Callback";
            //Provide the App Credentials. You get those by registering your app at dev.fitbit.com
            //Configure Fitbit authenticaiton request to perform a callback to this constructor's Callback method
            //var authenticator = new OAuth2Helper(appCredentials, Request.Url.GetLeftPart(UriPartial.Authority) + "/Fitbit/Callback");
            //var authenticator = new OAuth2Helper(appCredentials, "https://samplewebmvcoauth220201016123544.azurewebsites.net/Fitbit/Callback");
            var authenticator = new OAuth2Helper(appCredentials, link);
            string[] scopes = new string[] { "activity", "heartrate", "location", "nutrition", "profile", "settings", "sleep", "social", "weight" };

            string authUrl = authenticator.GenerateAuthUrl(scopes, null);

            return Redirect(authUrl);
        }

        //Final step. Take this authorization information and use it in the app
        public async Task<ActionResult> Callback()
        {
            FitbitAppCredentials appCredentials = (FitbitAppCredentials)Session["AppCredentials"];
            string callbackurl = Request.Url.GetLeftPart(UriPartial.Authority) + "/Fitbit/Callback";
            callbackurl = "https://samplewebmvcoauth220201016123544.azurewebsites.net/Fitbit/Callback";
            //callbackurl = "http://localhost/SampleWebMVCOAuth2/Fitbit/Callback";
            //https://samplewebmvcoauth220201016123544.azurewebsites.net/Fitbit/Callback

            //http://localhost/SampleWebMVCOAuth2/Fitbit/Authorize
            //http://localhost/SampleWebMVCOAuth2/Fitbit/Callback

            //var authenticator = new OAuth2Helper(appCredentials, Request.Url.GetLeftPart(UriPartial.Authority) + "/Fitbit/Callback"); 
            var authenticator = new OAuth2Helper(appCredentials, callbackurl);// "http://localhost/SampleWebMVCOAuth2/Fitbit/Callback");
            string code = Request.Params["code"];

            OAuth2AccessToken accessToken = await authenticator.ExchangeAuthCodeForAccessTokenAsync(code);
           
           // System.IO.File.WriteAllText(@"D:\OneDrive - Microsoft\Nagendra\Exercise\FitBit\Token\Token.txt", JsonConvert.SerializeObject(accessToken));
            DataTable dt = new DataTable();
            dt.Columns.Add("AccessTokenType");
            dt.Columns.Add("AccessToken");
            DataColumn colDateTime = new DataColumn("TokenGeneratedTime");
            colDateTime.DataType = System.Type.GetType("System.DateTime");
            dt.Columns.Add(colDateTime);



            dt.TableName = "FitbitToken";
            DataRow dr = dt.NewRow();
            dr["AccessTokenType"] = "AccessToken";
            dr["AccessToken"] = JsonConvert.SerializeObject(accessToken);
            dr["TokenGeneratedTime"] = DateTime.Now;
            dt.Rows.Add(dr);
            DBUtilities.ConnectionString = ConnectionString;
           // DBUtilities.DeleteTableFromDatabase(dt);
            DBUtilities.InsertDatatableToDatabase(dt);

            //Store credentials in FitbitClient. The client in its default implementation manages the Refresh process
            var fitbitClient = GetFitbitClient(accessToken);

            ViewBag.AccessToken = accessToken;

            return View();

        }

        /// <summary>
        /// In this example we show how to explicitly request a token refresh. However, FitbitClient V2 on its default implementation provide an OOB automatic token refresh.
        /// </summary>
        /// <returns>A refreshed token</returns>
        public async Task<ActionResult> RefreshToken()
        {
            var fitbitClient = GetFitbitClient();

            ViewBag.AccessToken = await fitbitClient.RefreshOAuth2Token();

            return View("Callback");
        }

        public async Task<ActionResult> TestToken()
        {
            var fitbitClient = GetFitbitClient();

            ViewBag.AccessToken = fitbitClient.AccessToken;

            ViewBag.UserProfile = await fitbitClient.GetUserProfileAsync();

            return View("TestToken");
        }

        /*
        public string TestTimeSeries()
        {
            FitbitClient client = GetFitbitClient();

            var results = client.GetTimeSeries(TimeSeriesResourceType.DistanceTracker, DateTime.UtcNow.AddDays(-7), DateTime.UtcNow);

            string sOutput = "";
            foreach (var result in results.DataList)
            {
                sOutput += result.DateTime.ToString() + " - " + result.Value.ToString();
            }

            return sOutput;

        }
        
        public ActionResult LastWeekDistance()
        {
            FitbitClient client = GetFitbitClient();

            TimeSeriesDataList results = client.GetTimeSeries(TimeSeriesResourceType.Distance, DateTime.UtcNow.AddDays(-7), DateTime.UtcNow);

            return View(results);
        }
        */

        public async Task<ActionResult> LastWeekSteps()
        {

            FitbitClient client = GetFitbitClient();

            var response = await client.GetTimeSeriesIntAsync(TimeSeriesResourceType.Steps, DateTime.UtcNow.AddDays(-7), DateTime.UtcNow);

            return View(response);

        }
        /*
        //example using the direct API call getting all the individual logs
        public ActionResult MonthFat(string id)
        {
            DateTime dateStart = Convert.ToDateTime(id);

            FitbitClient client = GetFitbitClient();

            Fat fat = client.GetFat(dateStart, DateRangePeriod.OneMonth);

            if (fat == null || fat.FatLogs == null) //succeeded but no records
            {
                fat = new Fat();
                fat.FatLogs = new List<FatLog>();
            }
            return View(fat);

        }

        //example using the time series, one per day
        public ActionResult LastYearFat()
        {
            FitbitClient client = GetFitbitClient();

            TimeSeriesDataList fatSeries = client.GetTimeSeries(TimeSeriesResourceType.Fat, DateTime.UtcNow, DateRangePeriod.OneYear);

            return View(fatSeries);

        }

        //example using the direct API call getting all the individual logs
        public ActionResult MonthWeight(string id)
        {
            DateTime dateStart = Convert.ToDateTime(id);

            FitbitClient client = GetFitbitClient();

            Weight weight = client.GetWeight(dateStart, DateRangePeriod.OneMonth);

            if (weight == null || weight.Weights == null) //succeeded but no records
            {
                weight = new Weight();
                weight.Weights = new List<WeightLog>();
            }
            return View(weight);

        }

        //example using the time series, one per day
        public ActionResult LastYearWeight()
        {
            FitbitClient client = GetFitbitClient();

            TimeSeriesDataList weightSeries = client.GetTimeSeries(TimeSeriesResourceType.Weight, DateTime.UtcNow, DateRangePeriod.OneYear);

            return View(weightSeries);

        }

        /// <summary>
        /// This requires the Fitbit staff approval of your app before it can be called
        /// </summary>
        /// <returns></returns>
        public string TestIntraDay()
        {
            FitbitClient client = new FitbitClient(ConfigurationManager.AppSettings["FitbitConsumerKey"],
                ConfigurationManager.AppSettings["FitbitConsumerSecret"],
                Session["FitbitAuthToken"].ToString(),
                Session["FitbitAuthTokenSecret"].ToString());

            IntradayData data = client.GetIntraDayTimeSeries(IntradayResourceType.Steps, new DateTime(2012, 5, 28, 11, 0, 0), new TimeSpan(1, 0, 0));

            string result = "";

            foreach (IntradayDataValues intraData in data.DataSet)
            {
                result += intraData.Time.ToShortTimeString() + " - " + intraData.Value + Environment.NewLine;
            }

            return result;

        }

         */

        /// <summary>
        /// HttpClient and hence FitbitClient are designed to be long-lived for the duration of the session. This method ensures only one client is created for the duration of the session.
        /// More info at: http://stackoverflow.com/questions/22560971/what-is-the-overhead-of-creating-a-new-httpclient-per-call-in-a-webapi-client
        /// </summary>
        /// <returns></returns>
        private FitbitClient GetFitbitClient(OAuth2AccessToken accessToken = null)
        {
            if (Session["FitbitClient"] == null)
            {
                if (accessToken != null)
                {
                    var appCredentials = (FitbitAppCredentials)Session["AppCredentials"];
                    FitbitClient client = new FitbitClient(appCredentials, accessToken);
                    Session["FitbitClient"] = client;
                    return client;
                }
                else
                {
                    throw new Exception("First time requesting a FitbitClient from the session you must pass the AccessToken.");
                }

            }
            else
            {
                return (FitbitClient)Session["FitbitClient"];
            }
        }
    }
}