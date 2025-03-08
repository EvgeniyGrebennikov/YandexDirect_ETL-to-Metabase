drop table if exists ya_direct;

create table if not exists ya_direct (
                id serial primary key,
                Date date,
                CampaignId varchar,
                CampaignName varchar,
                CampaignType varchar,
                AdGroupId varchar,
                AdGroupName varchar,
                AdId varchar,
                AdFormat varchar,
                CriterionId varchar,
                Criterion varchar,
                CriterionType varchar,
                Impressions smallint,
                Clicks smallint,
                Cost numeric,
                AdNetworkType varchar,
                Placement varchar,
                AvgImpressionPosition decimal,
                AvgClickPosition decimal,
                Slot varchar,
                BounceRate decimal,
                AvgPageviews decimal,
                        form_order_callback smallint,
                        call smallint,
                        form_callback smallint,
                        purchase smallint,
                        form_call_manager smallint,
                        form_under_search smallint,
                Age varchar,
                Gender varchar,
                Device varchar,
                MobilePlatform varchar,
                LocationOfPresenceId varchar,
                LocationOfPresenceName varchar,
                TargetingLocationId varchar,
                TargetingLocationName varchar,
                RlAdjustmentId varchar,
                TargetingCategory varchar
);

truncate table ya_direct;

select *
from ya_direct yd 
limit 5;
