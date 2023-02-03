INSERT INTO {{ params.schema }}.{{ params.table }}
SELECT
    CASE
        WHEN app_id = 'id1325756279'
            AND (campaign ILIKE '%webios%'
                AND campaign NOT ILIKE '%webonb%'
                AND campaign NOT ILIKE '%aiweb%'
                 )
            AND media_source = 'Facebook Ads'          THEN 'FB l.prequel.app'

        WHEN campaign ILIKE '%aiweb%'
            AND media_source = 'Facebook Ads'          THEN 'aiweb'

        WHEN app_id = 'id1325756279'
            AND campaign ilike '%webonb%'
            AND media_source = 'Facebook Ads'          THEN 'web-onboarding iOS'

        WHEN app_id = 'com.prequel.app'
            AND campaign ilike '%awb%'
            AND media_source = 'Facebook Ads'          THEN 'web-onboarding Android'

        ELSE media_source
        END AS custom_source
     , CASE
           WHEN (
                            app_id = 'id1325756279'
                        AND (campaign ILIKE '%webios%'
                        AND campaign NOT ILIKE '%webonb%'
                        AND campaign NOT ILIKE '%aiweb%')
                        AND media_source = 'Facebook Ads')

               OR (campaign ILIKE '%aiweb%'
                   AND media_source = 'Facebook Ads')

               OR (app_id = 'id1325756279'
                   AND campaign ilike '%webonb%'
                   AND media_source = 'Facebook Ads')          THEN 'ios'

           WHEN app_id = 'com.prequel.app'
               AND campaign ilike '%awb%'
               AND media_source = 'Facebook Ads'          THEN 'android'

           ELSE if(campaign ILIKE '%android%', 'android', 'ios')

    END AS platform
     , app_id
     , `date`
     , agency
     , campaign
     , campaign_id
     , adset
     , adset_id
     , ad
     , ad_id
     , `channel`
     , geo
     , site_id
     , keywords
     , timezone
     , ad_account
     , cost
     , currency
     , impressions
     , clicks
     , reported_clicks
     , re_engagements
     , re_attributions
     , original_cost
     , original_currency
     , reported_impressions
     , installs
FROM {{ params.source_schema }}.{{ params.source_table }}