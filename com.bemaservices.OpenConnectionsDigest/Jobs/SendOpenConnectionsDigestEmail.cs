using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;

using Quartz;

using Rock;
using Rock.Jobs;
using Rock.Attribute;
using Rock.Communication;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using Rock.IpAddress;
using Rock.Logging;
using System.Text;
using System.Web;
using System.Runtime.Remoting.Messaging;


namespace com.bemaservices.OpenConnectionsDigest.Jobs
{
    [SystemCommunicationField( "System Communication",
        Description = "The system communication to use when sending reminders.",
        Key = AttributeKey.SystemCommunication,
        IsRequired = true,
        DefaultSystemCommunicationGuid = "F3694629-0C5E-4B13-BDCD-45B21480CC84",
        Order = 0 )]

    [CustomDropdownListField( "Send Using",
        Description = "Specifies how the reminder will be sent.",
        Key = AttributeKey.SendUsingConfiguration,
        ListSource = "1^Email,2^SMS,0^Recipient Preference",
        IsRequired = true,
        DefaultValue = "1",
        Order = 1 )]

    [CustomCheckboxListField( "Connection Opportunities",
        Description = "Select the connection opportunities you would like to include.",
        Key = AttributeKey.ConnectionOpportunities,
        ListSource = "SELECT Guid AS Value, Name AS Text FROM ConnectionOpportunity WHERE IsActive = 1;",
        IsRequired = false,
        Order = 2 )]

    [BooleanField( "Include All Requests", 
        Description = "Should the email digest include a line for every connection request?",
        Key = AttributeKey.IncludeAllRequests,
        IsRequired = false,
        DefaultBooleanValue = true,
        Order = 3)]

    [BooleanField( "Include Opportunity Breakdown", 
        Description = "Should the email digest include an opportunity breakdown?",
        Key = AttributeKey.IncludeOpportunityBreakdown,
        IsRequired = false,
        DefaultBooleanValue = true,
        Order = 4)]

    [GroupField( "Connection Group", 
        Description = "Members of this group who are connectors of any opportunity with connections assigned will be emailed.",
        Key = AttributeKey.ConnectionGroup,
        IsRequired = false,
        Order = 5)]

    [DisallowConcurrentExecution]
    public class SendOpenConnectionsDigestEmail : RockJob
    {
        #region Attribute Keys

        /// <summary>
        /// Keys to use for Block Attributes
        /// </summary>
        private static class AttributeKey
        {

            public const string ConnectionGroup = "ConnectionGroup";

            public const string ConnectionOpportunities = "ConnectionOpportunities";

            public const string SystemCommunication = "SystemCommunication";

            public const string IncludeOpportunityBreakdown = "IncludeOpportunityBreakdown";

            public const string IncludeAllRequests = "IncludeAllRequests";

            public const string SendUsingConfiguration = "SendUsingConfiguration";
        }

        #endregion Attribute Keys
        public override void Execute()
        {
            DateTime midnightToday = RockDateTime.Today.AddDays( 1 );
            var currentDateTime = RockDateTime.Now;
            var results = new StringBuilder();

            this.Result = "0 connection reminders sent.";

            var rockContext = new RockContext();
            var groupService = new GroupService( rockContext );
            var groupMemberService = new GroupMemberService( rockContext );
            var personService = new PersonService( rockContext );
            var connectionRequestService = new ConnectionRequestService( rockContext );
            var systemCommunicationService = new SystemCommunicationService( rockContext );

            // Get Job Information
            int jobId = Convert.ToInt16( this.ServiceJobId );
            var jobService = new ServiceJobService( rockContext );
            var job = jobService.Get( jobId );

            // Get System Communication
            SystemCommunication systemCommunication = null;
            var systemCommunicationGuid = GetAttributeValue( AttributeKey.SystemCommunication ).AsGuidOrNull();
            if ( systemCommunicationGuid != null )
            {
                systemCommunication = systemCommunicationService.Get( systemCommunicationGuid.Value );
            }

            if ( systemCommunication == null )
            {
                var warning = $"System Communication is required!";
                results.Append( FormatWarningMessage( warning ) );
                this.Result = results.ToString();
                throw new RockJobWarningException( warning );
            }

            var jobPreferredCommunicationType = ( CommunicationType ) GetAttributeValue( AttributeKey.SendUsingConfiguration ).AsInteger();
            var isSmsEnabled = MediumContainer.HasActiveSmsTransport() && !string.IsNullOrWhiteSpace( systemCommunication.SMSMessage );

            if ( jobPreferredCommunicationType == CommunicationType.SMS && !isSmsEnabled )
            {
                // If sms selected but not usable default to email.
                var errorMessage = $"The job is setup to send via SMS but either SMS isn't enabled or no SMS message was found in system communication {systemCommunication.Title}.";
                HandleErrorMessage( errorMessage );
            }

            if ( jobPreferredCommunicationType != CommunicationType.Email && string.IsNullOrWhiteSpace( systemCommunication.SMSMessage ) )
            {
                var warning = $"No SMS message found in system communication {systemCommunication.Title}. All attendance reminders were sent via email.";
                results.Append( FormatWarningMessage( warning ) );
                jobPreferredCommunicationType = CommunicationType.Email;
            }

            // Get Connection Requests
            var connectionRequestsQry = connectionRequestService.Queryable().Where( cr =>
                                           cr.ConnectorPersonAliasId != null &&
                                           (
                                                cr.ConnectionState == ConnectionState.Active ||
                                                (
                                                    cr.ConnectionState == ConnectionState.FutureFollowUp &&
                                                    cr.FollowupDate.HasValue &&
                                                    cr.FollowupDate.Value < midnightToday
                                                )
                                            )
                                        );

            // Filter By Connection Opportunities
            var connectionOpportunities = GetAttributeValue( AttributeKey.ConnectionOpportunities ).SplitDelimitedValues().AsGuidList();
            if ( connectionOpportunities.Any() )
            {
                connectionRequestsQry = connectionRequestsQry.Where( cr => connectionOpportunities.Contains( cr.ConnectionOpportunity.Guid ) );
            }

            // If we have a group of connectors, limit it to them.
            var group = groupService.GetByGuid( GetAttributeValue( AttributeKey.ConnectionGroup ).AsGuid() );
            if ( group != null )
            {
                List<int> groupconnectorPersonIds = group.ActiveMembers().SelectMany( gm => gm.Person.Aliases ).Select( a => a.Id ).ToList();
                connectionRequestsQry = connectionRequestsQry.Where( cr => cr.ConnectorPersonAliasId.HasValue && groupconnectorPersonIds.Contains( cr.ConnectorPersonAliasId.Value ) );
            }

            // Now get all the connection data for everyone.
            SendMessageResult connectionRemindersResults = SendConnectionReminders( currentDateTime, personService, job, systemCommunication, jobPreferredCommunicationType, connectionRequestsQry );

            results.AppendLine( $"{connectionRemindersResults.MessagesSent} connection reminders sent." );
            results.Append( FormatWarningMessages( connectionRemindersResults.Warnings ) );
            this.Result = results.ToString();

            HandleErrorMessages( connectionRemindersResults.Errors );

        }

        private SendMessageResult SendConnectionReminders( DateTime currentDateTime, PersonService personService, ServiceJob job, SystemCommunication systemCommunication, CommunicationType jobPreferredCommunicationType, IQueryable<ConnectionRequest> connectionRequestsQry )
        {
            var connectionRemindersResults = new SendMessageResult();
            var connectionRequestConnectors = connectionRequestsQry.GroupBy( cr => cr.ConnectorPersonAlias.PersonId );
            foreach ( var connectionRequestConnector in connectionRequestConnectors )
            {
                Person person = personService.Get( connectionRequestConnector.Key );
                var mediumType = Rock.Model.Communication.DetermineMediumEntityTypeId(
                                    ( int ) CommunicationType.Email,
                                    ( int ) CommunicationType.SMS,
                                    ( int ) CommunicationType.PushNotification,
                                    jobPreferredCommunicationType,
                                    person.CommunicationPreference );

                List<ConnectionOpportunity> opportunities = connectionRequestConnector.Select( a => a.ConnectionOpportunity ).Distinct().ToList();
                var newConnectionRequests = connectionRequestConnector.Where( cr => cr.CreatedDateTime >= job.LastSuccessfulRunDateTime ).GroupBy( cr => cr.ConnectionOpportunityId ).ToList();

                // Get all the idle connections
                var idleConnectionRequests = connectionRequestConnector
                                    .Where( cr => (
                                            ( cr.ConnectionRequestActivities.Any() && cr.ConnectionRequestActivities.Max( ra => ra.CreatedDateTime ) < currentDateTime.AddDays( -cr.ConnectionOpportunity.ConnectionType.DaysUntilRequestIdle ) ) )
                                            || ( !cr.ConnectionRequestActivities.Any() && cr.CreatedDateTime < currentDateTime.AddDays( -cr.ConnectionOpportunity.ConnectionType.DaysUntilRequestIdle ) )
                                           )
                                    .Select( a => new { ConnectionOpportunityId = a.ConnectionOpportunityId, Id = a.Id } )
                                    .GroupBy( cr => cr.ConnectionOpportunityId ).ToList();

                // get list of requests that have a status that is considered critical.
                var criticalConnectionRequests = connectionRequestConnector
                                        .Where( r =>
                                            r.ConnectionStatus.IsCritical
                                        )
                                        .Select( a => new { ConnectionOpportunityId = a.ConnectionOpportunityId, Id = a.Id } )
                                        .GroupBy( cr => cr.ConnectionOpportunityId ).ToList();

                var mergeFields = Rock.Lava.LavaHelper.GetCommonMergeFields( null, person );
                mergeFields.Add( "Requests", connectionRequestConnector.Select( c => c ).ToList() );
                mergeFields.Add( "ConnectionOpportunities", opportunities );
                mergeFields.Add( "ConnectionRequests", connectionRequestConnector.GroupBy( cr => cr.ConnectionOpportunity ).ToList() );
                mergeFields.Add( "NewConnectionRequests", newConnectionRequests );
                mergeFields.Add( "IdleConnectionRequestIds", idleConnectionRequests );
                mergeFields.Add( "CriticalConnectionRequestIds", criticalConnectionRequests );
                mergeFields.Add( "Person", person );
                mergeFields.Add( "LastRunDate", job.LastSuccessfulRunDateTime );
                mergeFields.Add( "IncludeOpportunityBreakdown", GetAttributeValue( AttributeKey.IncludeOpportunityBreakdown ).AsBoolean() );
                mergeFields.Add( "IncludeAllRequests", GetAttributeValue( AttributeKey.IncludeAllRequests ).AsBoolean() );


                var sendResult = CommunicationHelper.SendMessage( person, mediumType, systemCommunication, mergeFields );

                connectionRemindersResults.MessagesSent += sendResult.MessagesSent;
                connectionRemindersResults.Errors.AddRange( sendResult.Errors );
                connectionRemindersResults.Warnings.AddRange( sendResult.Warnings );
            }

            return connectionRemindersResults;
        }

        private StringBuilder FormatWarningMessage( string warning )
        {
            var errorMessages = new List<string> { warning };
            return FormatMessages( errorMessages, "Warning" );
        }

        private StringBuilder FormatWarningMessages( List<string> warnings )
        {
            return FormatMessages( warnings, "Warning" );
        }

        private void HandleErrorMessage( string errorMessage )
        {
            if ( errorMessage.IsNullOrWhiteSpace() )
            {
                return;
            }

            var errorMessages = new List<string> { errorMessage };
            HandleErrorMessages( errorMessages );
        }

        /// <summary>
        /// Handles the error messages. Throws an exception if there are any items in the errorMessages parameter
        /// </summary>
        /// <param name="errorMessages">The error messages.</param>
        private void HandleErrorMessages( List<string> errorMessages )
        {
            if ( errorMessages.Any() )
            {
                StringBuilder sb = new StringBuilder( this.Result.ToString() );
                sb.Append( FormatMessages( errorMessages, "Error" ) );

                var resultMessage = sb.ToString();
                this.Result = resultMessage;
                var exception = new Exception( resultMessage );

                HttpContext context2 = HttpContext.Current;
                ExceptionLogService.LogException( exception, context2 );
                throw exception;
            }
        }

        private StringBuilder FormatMessages( List<string> messages, string label )
        {
            StringBuilder sb = new StringBuilder();
            if ( messages.Any() )
            {
                var pluralizedLabel = label.PluralizeIf( messages.Count > 1 );
                sb.AppendLine( $"{messages.Count} {pluralizedLabel}:" );
                messages.ForEach( w => { sb.AppendLine( w ); } );
            }
            return sb;
        }
    }
}