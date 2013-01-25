# Galaxya Loader

Chargement de données CSV dans une base PostgreSQL. Les données sont
proprement encodée en UTF8 et seuls les champs dates doivent recevoir un
traitement particulier.

Les dates `0000-00-00` deviennent `NULL`. Les dates `0000-00-00 00:00:00`
aussi.

## Installation et dependances

Programme Common Lisp utilisant [SBCL](http://sbcl.org/) et
[Quicklisp](http://www.quicklisp.org/beta/).

    apt-get install sbcl
	wget http://beta.quicklisp.org/quicklisp.lisp
	sbcl --load quicklisp.lisp
	* (quicklisp-quickstart:install)
	* (ql:add-to-init-file)

Ensuite il faut récupérer les sources du projet dans
`~/quicklisp/local-projects/`, afin de pouvoir faire :

    sbcl
	* (ql:quickload :galaxa-loader)
	* (in-package :galaxya-loader)
	* (stream-database-tables "weetix")

Ou bien directement :

    * (load-all-databases)

## Example d'usage

Un chargement complet d'une base de données, en streaming directement depuis
MySQL vers PostgreSQL :

~~~
GALAXYA-LOADER> (stream-database-tables "weetix")
                    table name       rows       time
------------------------------  ---------  ---------
                      ab_tests  skip, unknown table in MySQL database
                  affiliations       1018   0m0s.429
                     blacklist        352   0m0s.365
                       blogtop      41451   0m3s.697
                  ca_sms_boost        447   0m0s.515
                         codes    3344498  4m42s.328
                  codes_buffer         15   0m0s.312
              codes_distribues     366392  0m39s.493
      codes_distribues_demande        795   0m0s.787
                    codes_temp          3   0m0s.394
                 codes_vipplus        859   0m0s.956
                       com_log      39703   0m2s.869
               com_partenaires         19   0m0s.436
            commandes_boutique      78717   0m5s.966
                  commentaires       7009   0m0s.631
               communaute_amis      62948   0m5s.216
          communaute_blacklist         67   0m0s.519
      communaute_messagerie_id       4448   0m0s.606
communaute_messagerie_messages       9855    0m1s.91
            communaute_no_show       9018   0m0s.481
               concours_weetix        773   0m0s.645
                       coupons         71   0m0s.470
                           css     163463  0m15s.919
                     documents      21956  0m10s.608
                documents_lots      23118   0m2s.130
                documents_pays      31201   0m2s.278
                 echange_votes        251   0m0s.447
              etransactions_id          1    0m0s.51
                      gagnants      21457   0m2s.462
                 gagnants_docs        321   0m0s.435
            gains_affiliations      17280   0m1s.902
                gains_concours      24728   0m3s.219
              gains_webmasters     581093  0m43s.599
                    happy_hour          1   0m0s.155
                       interco        120   0m0s.424
                            ip       1603   0m0s.684
                       litiges        172   0m0s.690
                   livemessage      10719   0m0s.778
                   log_actions    1116761  1m24s.826
               log_actions_vip    2912106  2m15s.702
          log_facebook_credits        475   0m2s.370
              log_flood_action        453   0m0s.350
             log_modifications       4450   0m0s.484
                      log_mpme      34615    0m3s.92
                    log_points     432229  0m28s.561
                       log_sms      31766   0m2s.631
                   log_vipplus      18603   0m1s.548
                     log_votes     627462  0m26s.465
             log_votes_details    2873684  1m57s.461
             log_votes_manuels       4853   0m0s.446
                       membres      26752   0m3s.261
               membres_referer       9628   0m0s.787
          membres_verification      13976   0m1s.113
                   membres_vip     118192   0m8s.790
          membres_vip_interets     101571   0m2s.825
                      muzictop      16686   0m1s.476
           newsletters_clients        387   0m0s.804
                       numeros        133   0m0s.752
                   oppositions        518   0m0s.340
                     paiements       8231   0m0s.771
                     peopletop      56295   0m5s.311
                         plans          3   0m0s.347
            plans_reversements        114   0m0s.527
                  prestataires         17   0m0s.343
                   promo_boost      74946   0m4s.967
                  reversements      21940   0m1s.600
                 sav_questions         22   0m0s.353
                    sav_themes          5   0m0s.303
             sav_vip_questions         40   0m0s.300
                sav_vip_themes          7   0m0s.308
                      services         29   0m0s.275
                     short_url    1820123   1m6s.423
                short_url_temp      97495   0m7s.341
                     sms_boost       5552   0m0s.979
                 sms_boost_log    1008752   1m5s.178
                       startop     399326   0m19s.53
            temp_codes_annules        918   0m0s.391
                      videotop       1352   0m0s.752
               vip_alertes_sms        124   0m0s.491
                     vip_bonus       6816   0m0s.465
               vip_classements      44536   0m3s.834
    vip_classements_categories         17   0m0s.839
         vip_infos_classements         31   0m0s.509
                    vip_profil      40384   0m3s.411
      vip_regularisation_codes      15535    0m1s.95
             vip_sollicitation     106368   0m6s.101
          vip_verification_tel       8116   0m0s.660
                        webtop      21983    0m2s.74
                     zarchives      16296   0m1s.158
             zarchives_blogtop     156067  0m10s.502
            zarchives_muzictop     210174   0m8s.201
           zarchives_peopletop      59026   0m5s.560
             zarchives_startop     360187  0m19s.334
            zarchives_videotop       6917   0m0s.742
              zarchives_webtop     146387  0m10s.117
------------------------------  ---------  ---------
          Total streaming time   17905373  18m28s.686
17905373
1108.686
~~~

