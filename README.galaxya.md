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
                    table name       rows       time
------------------------------  ---------  ---------
                      ab_tests  skip, unknown table in MySQL database
                  affiliations       1018   0m0s.440
                     blacklist        352   0m0s.348
                       blogtop      41451   0m1s.564
                  ca_sms_boost        447   0m0s.520
                         codes    3344498   4m8s.535
                  codes_buffer         15   0m8s.638
              codes_distribues     366392  0m24s.908
      codes_distribues_demande        795     0m1s.3
                    codes_temp          3  0m17s.195
                 codes_vipplus        859    0m8s.65
                       com_log      39703   0m2s.122
               com_partenaires         19   0m0s.360
            commandes_boutique      78717   0m2s.363
                  commentaires       7009   0m0s.770
               communaute_amis      62948   0m1s.452
          communaute_blacklist         67   0m0s.619
      communaute_messagerie_id       4448   0m0s.606
communaute_messagerie_messages       9855   0m0s.907
            communaute_no_show       9018   0m0s.437
               concours_weetix        773   0m0s.609
                       coupons         71   0m0s.833
                           css     163463   0m3s.724
                     documents      21956   0m2s.148
                documents_lots      23118   0m0s.909
                documents_pays      31201   0m0s.627
                 echange_votes        251   0m0s.436
              etransactions_id          1    0m0s.54
                      gagnants      21457   0m1s.132
                 gagnants_docs        321   0m0s.543
            gains_affiliations      17280   0m1s.494
                gains_concours      24728   0m1s.547
              gains_webmasters     581093  0m12s.612
                    happy_hour          1   0m0s.501
                       interco        120   0m0s.834
                            ip       1603   0m1s.290
                       litiges        172   0m1s.487
                   livemessage      10719   0m1s.428
                   log_actions    1116761  0m31s.882
               log_actions_vip    2912106  1m31s.668
          log_facebook_credits        475   0m0s.908
              log_flood_action        453   0m0s.438
             log_modifications       4450   0m0s.458
                      log_mpme      34615   0m1s.890
                    log_points     432229   0m4s.337
                       log_sms      31766   0m1s.285
                   log_vipplus      18603   0m0s.714
                     log_votes     627462  0m12s.462
             log_votes_details    2873684  1m24s.824
             log_votes_manuels       4853   0m0s.530
                       membres      26752   0m2s.990
               membres_referer       9628   0m0s.564
          membres_verification      13976   0m0s.573
                   membres_vip     118192   0m8s.420
          membres_vip_interets     101571   0m0s.669
                      muzictop      16686    0m1s.18
           newsletters_clients        387   0m2s.914
                       numeros        133   0m0s.772
                   oppositions        518   0m0s.356
                     paiements       8231   0m0s.612
                     peopletop      56295   0m1s.781
                         plans          3   0m0s.401
            plans_reversements        114   0m0s.544
                  prestataires         17   0m0s.315
                   promo_boost      74946   0m1s.769
                  reversements      21940   0m0s.744
                 sav_questions         22   0m0s.573
                    sav_themes          5   0m0s.407
             sav_vip_questions         40   0m0s.381
                sav_vip_themes          7   0m0s.319
                      services         29   0m0s.284
                     short_url    1820123  0m39s.355
                short_url_temp      97495   0m1s.174
                     sms_boost       5552   0m1s.871
                 sms_boost_log    1008752  0m44s.848
                       startop     399326   0m7s.343
            temp_codes_annules        918   0m0s.342
                      videotop       1352   0m0s.805
               vip_alertes_sms        124   0m0s.502
                     vip_bonus       6816   0m0s.449
               vip_classements      44536   0m2s.592
    vip_classements_categories         17   0m0s.348
         vip_infos_classements         31   0m0s.500
                    vip_profil      40384     0m1s.9
      vip_regularisation_codes      15535   0m0s.735
             vip_sollicitation     106368   0m1s.224
          vip_verification_tel       8116   0m0s.637
                        webtop      21983   0m1s.195
                     zarchives      16296   0m0s.797
             zarchives_blogtop     156067   0m6s.455
            zarchives_muzictop     210174   0m4s.553
           zarchives_peopletop      59026   0m2s.494
             zarchives_startop     360187   0m7s.223
            zarchives_videotop       6917   0m0s.787
              zarchives_webtop     146387   0m3s.196
------------------------------  ---------  ---------
          Total streaming time   17905373  12m21s.296
17905373
741.296
~~~

Et en passant par des exports sur fichier temporaire au format COPY TEXT de
PostgreSQL:

~~~
GALAXYA-LOADER> (load-database-tables-from-file "weetix")
                    table name       rows       time
------------------------------  ---------  ---------
                      ab_tests   skip, unknown table in MySQL database
                  affiliations       1018   0m0s.751
                     blacklist        352   0m0s.416
                       blogtop      41451   0m2s.745
                  ca_sms_boost        447   0m0s.563
                         codes    3344498  5m45s.511
                  codes_buffer         15   0m0s.425
              codes_distribues     366392  0m32s.598
      codes_distribues_demande        795   0m0s.562
                    codes_temp          3   0m0s.453
                 codes_vipplus        859   0m0s.994
                       com_log      39703   0m2s.388
               com_partenaires         19   0m0s.420
            commandes_boutique      78717   0m4s.919
                  commentaires       7009   0m2s.881
               communaute_amis      62948   0m8s.724
          communaute_blacklist         67   0m1s.236
      communaute_messagerie_id       4448   0m1s.522
communaute_messagerie_messages       9855   0m1s.614
            communaute_no_show       9018   0m0s.437
               concours_weetix        773   0m1s.305
                       coupons         71   0m0s.525
                           css     163463   0m5s.604
                     documents      21956   0m3s.788
                documents_lots      23118   0m1s.333
                documents_pays      31201   0m0s.930
                 echange_votes        251   0m0s.473
              etransactions_id          1    0m0s.99
                      gagnants      21457    0m2s.85
                 gagnants_docs        321   0m0s.489
            gains_affiliations      17280   0m1s.736
                gains_concours      24728   0m1s.889
              gains_webmasters     581093   0m20s.38
                    happy_hour          1   0m0s.147
                       interco        120   0m0s.457
                            ip       1603   0m0s.787
                       litiges        172   0m0s.750
                   livemessage      10719   0m1s.632
                   log_actions    1116761  1m23s.781
               log_actions_vip    2912106  3m48s.206
          log_facebook_credits        475   0m1s.178
              log_flood_action        453   0m0s.434
             log_modifications       4450   0m0s.952
                      log_mpme      34615   0m2s.859
                    log_points     432229  0m10s.778
                       log_sms      31766   0m2s.666
                   log_vipplus      18603   0m1s.145
                     log_votes     627462  0m25s.411
             log_votes_details    2873684  2m54s.984
             log_votes_manuels       4853   0m0s.600
                       membres      26752   0m6s.852
               membres_referer       9628   0m0s.832
          membres_verification      13976   0m0s.815
                   membres_vip     118192  0m18s.340
          membres_vip_interets     101571   0m3s.513
                      muzictop      16686   0m1s.367
           newsletters_clients        387   0m0s.941
                       numeros        133   0m0s.832
                   oppositions        518   0m0s.364
                     paiements       8231   0m0s.984
                     peopletop      56295   0m3s.730
                         plans          3   0m0s.375
            plans_reversements        114   0m0s.590
                  prestataires         17   0m0s.334
                   promo_boost      74946   0m3s.559
                  reversements      21940    0m1s.25
                 sav_questions         22   0m0s.541
                    sav_themes          5   0m0s.346
             sav_vip_questions         40   0m0s.382
                sav_vip_themes          7   0m0s.360
                      services         29   0m0s.317
                     short_url    1820123   0m55s.47
                short_url_temp      97495   0m4s.735
                     sms_boost       5552   0m1s.699
                 sms_boost_log    1008752  1m30s.472
                       startop     399326  0m16s.981
            temp_codes_annules        918   0m0s.532
                      videotop       1352   0m0s.822
               vip_alertes_sms        124   0m0s.577
                     vip_bonus       6816   0m0s.525
               vip_classements      44536   0m6s.669
    vip_classements_categories         17     0m1s.2
         vip_infos_classements         31   0m0s.658
                    vip_profil      40384   0m2s.545
      vip_regularisation_codes      15535   0m0s.809
             vip_sollicitation     106368   0m2s.858
          vip_verification_tel       8116   0m0s.791
                        webtop      21983   0m1s.766
                     zarchives      16296    0m1s.26
             zarchives_blogtop     156067   0m7s.121
            zarchives_muzictop     210174   0m8s.436
           zarchives_peopletop      59026    0m3s.53
             zarchives_startop     360187  0m13s.473
            zarchives_videotop       6917   0m1s.187
              zarchives_webtop     146387   0m7s.484
------------------------------  ---------  ---------
      Total export+import time   17905373  21m2s.887
17905373
1262.887
~~~
