/*
The company YellowHead manages ad campaigns on behalf of GSN for Bingo. We asked them to put YH at the beginning
of the campaign names so we can easily identify when an install is attributable to a campaign in their budget.
One of the campaigns does not have YH in the beginning so this script fixes that.
 */


UPDATE bash.bingo_installs
   SET source_name = 'FACEBOOK'
 WHERE installed_on > '2018-12-01'
   AND (campaign_name = 'Adquant_BingoBash_47277_iPad_US,CA_35-65_PURCHASE_2017-12-03_AEO'
        OR site LIKE '%YH%')
   AND source_name = 'ADQUANT';

UPDATE bash.bingo_installs
   SET source_name = 'ADQUANT'
 WHERE installed_on > '2018-12-01'
   AND (campaign_name = 'Adquant_BingoBash_iPhone_US_25-65_KW-Bingo_Value_0112'
    OR campaign_name = 'Adquant_BingoBash_47277_iPad_US,CA_35-65_PURCHASE_2017-12-03_AEO'
    OR campaign_name = 'BingoBash.iPhone.US+CA.F.25.AEO.bingo_KW_18092017_AEO')
   AND source_name = 'FACEBOOK';

UPDATE bash.bingo_installs
   SET source_name = 'ADQUANT'
 WHERE installed_on > '2018-12-01'
   AND (kochava_tracker_name = 'Adquant_Bingo_iPhone_Internal'
    OR kochava_tracker_name = 'Adquant_Bingo_Android_Internal')
   AND source_name IS NULL;

/*
 Ownership of several YellowHead campaigns has been transferred to the internal team.
 The adset name starts with YH_ (see query above) so we will use the campaign name,
 at the end of the campaign name is a tracking code, this will be used to make our update selective.
*/

UPDATE bash.bingo_installs
SET source_name = 'ADQUANT'
WHERE  installed_on > '2019-02-28'
   AND source_name = 'FACEBOOK'
   AND (adset_name LIKE '%_163388'         -- ipad
       OR adset_name LIKE '%_175226'       -- android
       OR adset_name LIKE '%_13033812641') -- iphone
;
