/*
For bash UA an external company called YellowHead runs facebook ads.
We are going to start having facebook campaigns managed internally by the
UA team in Israel. In the upstream pipeline that builds bash.bingo_intsalls
the kochava tracker name is used to determine the source name. This strategy will
not work for facebook ads because the external (YellowHead) and internal (GSN Israel)
teams are both using the facebook ad manager called AdQuant. This simple script
makes the assumption that YellowHead included YH_ at the beginning of every
campaign name.
 */

UPDATE bash.bingo_installs
   SET source_name =
       CASE
           WHEN campaign_name LIKE 'YH%'
            THEN 'FACEBOOK'
           WHEN campaign_name IS NOT NULL
            THEN 'ADQUANT'
           ELSE
            NULL
       END
WHERE source_name IN ('FACEBOOK', 'ADQUANT')
  AND installed_on > '2018-11-20';
