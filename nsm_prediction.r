## First specify the packages of interest
packages = c("tidyverse", "timetk",
             "modeltime", "miniUI", "shiny", "shinyFiles", "lubridate", "DBI","dbplyr",
             "tidymodels", "parsnip", "rsample" )

## Now load or install&load all
package.check <- lapply(
  packages,
  FUN = function(x) {
    if (!require(x, character.only = TRUE)) {
      install.packages(x, dependencies = TRUE)
      library(x, character.only = TRUE)
    }
  }
)




con <- DBI::dbConnect(odbc::odbc(),
                      #Snowflake
                      #SnowflakeDSIIDriver
                      Driver       = "SnowflakeDSIIDriver",
                      Server       = "ed87949.us-east-1.snowflakecomputing.com",

                      UID          = rstudioapi::askForPassword("Database user"),
                      PWD          = rstudioapi::askForPassword("Database password"),
                      Database     = "EDW",
                      Warehouse    = "shiny_app",
                      Schema       = "dim"
                      #,
                      #authenticator = "externalbrowser"
)
mywh <- DBI::dbSendQuery(con, 'use role shiny_app_role')
mywh <- DBI::dbSendQuery(con, 'use warehouse shiny_app')


mydata <- DBI::dbGetQuery(con, "
WITH base_actions AS (
SELECT
        stcd.STUDENT_USER_ID  AS student_user_id,
        stcd.TEACHER_USER_ID  AS teacher_user_id,
        stcd.ACTION_DATE_KEY AS action_date_key,
        stcd.action_date AS action_date,
        COALESCE(SUM(CASE WHEN (stcd.STUDENT_POWER_WORD_CLICKS  > 0)
                     THEN stcd.STUDENT_POWER_WORD_CLICKS  ELSE NULL END), 0)
                     AS student_power_word_clicks,
        COALESCE(SUM(CASE WHEN (stcd.STUDENT_POWER_WORD_ACTIVITIES  > 0)
                     THEN ( stcd.STUDENT_POWER_WORD_ACTIVITIES  )  ELSE NULL END), 0)
                     AS power_word_activities,
        COUNT(DISTINCT CASE WHEN (stcd.STUDENT_TAKE_QUIZ  > 0)
                      THEN ( stcd.STUDENT_USER_ID  ) || ( stcd.ARTICLE_LEVEL_ID  )  ELSE NULL END)
                      AS students_taking_quizzes,

        COALESCE(COUNT(CASE WHEN ((UPPER(stcd.CONTENT_TYPE ) = UPPER('Interactive Video')))
                     AND (stcd.STUDENT_INTERACTIVE_VIDEO_QUESTIONS_ANSWERED  > 0)
                     AND ((((stcd.STUDENT_USER_ID) IS NOT NULL)))
                     THEN ( 1 ) ELSE NULL END), 0)
                     AS student_interactive_video_quiz_questions_answered,

        COALESCE(SUM(CAST((stcd.STUDENT_RESPOND_WRITE_PROMPT) AS DOUBLE PRECISION)), 0)
                     AS sum_of_student_respond_write_prompt,
        COUNT(DISTINCT CASE WHEN (stcd.STUDENT_PRINTS  > 0)
                     THEN ( stcd.ARTICLE_HEADER_ID  ) || ( stcd.STUDENT_USER_ID  )  ELSE NULL END)
                     AS student_article_header_prints,
        COALESCE(SUM(CASE WHEN (stcd.STUDENT_HIGHLIGHT  > 0)
                     THEN stcd.STUDENT_HIGHLIGHT  ELSE NULL END), 0)
                     AS student_highlights,
        COALESCE(SUM(CAST((stcd.STUDENT_TAKE_QUIZ)
                     AS DOUBLE PRECISION)), 0)
                     AS sum_of_student_take_quiz,

        COUNT(DISTINCT CASE WHEN (stcd.TEACHER_PRINTS > 0)
                     THEN ( stcd.ARTICLE_HEADER_ID  ) || ( stcd.TEACHER_USER_ID  )  ELSE NULL END)
                     AS teacher_article_header_print,
        COALESCE(SUM(( CASE WHEN ( (stcd.STUDENT_INTERACTIVE_VIDEO_PLAY_CLICKS) > 0
                      AND (stcd.STUDENT_INTERACTIVE_VIDEO_ACCRUED_PLAY_TIME  > 30)) THEN 1 END  ) ), 0)
                     AS student_video_plays_over_30_seconds

  FROM
    WIDE.STUDENT_TEACHER_CONTENT_BY_DATE   AS stcd
  WHERE (
      STUDENT_POWER_WORD_ACTIVITIES > 0
      OR
      STUDENT_POWER_WORD_CLICKS > 0
      OR
      STUDENT_POWER_WORD_ACTIVITIES > 0
      OR
      STUDENT_TAKE_QUIZ > 0
      OR
      STUDENT_INTERACTIVE_VIDEO_QUESTIONS_ANSWERED > 0
      OR
      STUDENT_RESPOND_WRITE_PROMPT > 0
      OR
      STUDENT_PRINTS > 0
      OR
      STUDENT_HIGHLIGHT > 0
      OR
      STUDENT_TAKE_QUIZ > 0
      OR
      TEACHER_PRINTS > 0
      OR
      STUDENT_INTERACTIVE_VIDEO_PLAY_CLICKS > 0
      )
  AND
    ACTION_DATE_KEY  >=  20180701
GROUP BY
      student_user_id,
      teacher_user_id,
      ACTION_DATE_KEY,
      action_date

      )


, base_aggregate_filter AS (
  SELECT
      bsf.action_date,
      SUM(IFNULL(bsf.student_video_plays_over_30_seconds,0)) AS video_plays_over_30_seconds,
      SUM(IFNULL(bsf.student_power_word_clicks,0)) AS student_power_word_clicks,
      SUM(IFNULL(bsf.power_word_activities,0)) AS power_word_activities,
      SUM(IFNULL(bsf.students_taking_quizzes,0)) AS students_taking_quizzes,
      SUM(IFNULL(bsf.student_interactive_video_quiz_questions_answered,0)) AS student_interactive_video_quiz_questions_answered,
      SUM(IFNULL(bsf.SUM_OF_STUDENT_RESPOND_WRITE_PROMPT,0)) AS SUM_OF_STUDENT_RESPOND_WRITE_PROMPT,
      SUM(IFNULL(bsf.student_article_header_prints,0)) AS student_article_header_prints,
      SUM(IFNULL(bsf.student_highlights,0)) AS student_highlights,
      SUM(IFNULL(bsf.sum_of_student_take_quiz,0)) AS sum_of_student_take_quiz,
      SUM(IFNULL(bsf.teacher_article_header_print,0)) AS teacher_article_header_prints
 FROM  base_actions AS bsf

 WHERE
      ( bsf.student_user_id IS NOT NULL
      OR bsf.teacher_user_id IS NOT NULL)
      AND
      ( bsf.action_date IS NOT NULL)
GROUP BY
      bsf.action_date
)
, final_base_actions AS (
  SELECT action_date,
      SUM(video_plays_over_30_seconds) +
      SUM(student_power_word_clicks) +
      SUM(power_word_activities) +
      SUM(students_taking_quizzes) +
      SUM(student_interactive_video_quiz_questions_answered) +
      SUM(SUM_OF_STUDENT_RESPOND_WRITE_PROMPT) +
      SUM(student_article_header_prints) +
      SUM(student_highlights) +
      SUM(sum_of_student_take_quiz) +
      SUM(teacher_article_header_prints) AS number_of_student_base_nsm_activities
  FROM
      base_aggregate_filter baf
  JOIN
      dim.calendar cal
      ON baf.action_date=cal.date
  GROUP BY
      action_date
   )
, base_user_scroll_depth AS (
SELECT
      student_user_id AS user_id,
      action_date,
      article_header_id,
      student_license_type AS license_type,
      SUM(student_unique_total_scroll_thresholds) AS scroll_depth
FROM
      wide.student_teacher_content_by_date
WHERE
      action_date >= '2018-07-01'
GROUP BY
      student_user_id,
      action_date,
      article_header_id,
      student_license_type
      )
, user_scroll_depth AS  (
SELECT
      ua.user_id,
      ua.action_date,
      ua.article_header_id,
      ua.license_type,
      CASE WHEN ua.scroll_depth >= 2 THEN 1 ELSE 0 END AS reached_25,
      CASE WHEN ua.scroll_depth >= 3 THEN 1 ELSE 0 END AS reached_50,
      CASE WHEN ua.scroll_depth >= 4 THEN 1 ELSE 0 END AS reached_75,
      CASE WHEN ua.scroll_depth >= 5 THEN 1 ELSE 0 END AS reached_100
FROM
      base_user_scroll_depth ua
      )
, final_engaged_article AS (
SELECT    stcd.action_date,
          COUNT(DISTINCT CASE WHEN (stcd.student_ARTICLE_LEVEL_VIEWS  > 0)
          THEN ( stcd.ARTICLE_HEADER_ID  )||'&'||( stcd.student_user_id  )  ELSE NULL END)
          AS student_unique_article_header_views
FROM WIDE.STUDENT_TEACHER_CONTENT_BY_DATE AS stcd

-- Adding scroll_depth limit the dat to after 2021
LEFT JOIN user_scroll_depth AS usd
ON stcd.student_user_id=usd.user_id
AND stcd.action_date=usd.action_date
WHERE stcd.action_date >= '2018-07-01'
AND
((stcd.student_READING_TIME_IN_SECONDS ) >= 30
OR reached_75=1 )
GROUP BY
        stcd.action_date
    )
SELECT fba.action_date,
       cal.date_key,
      SUM(number_of_student_base_nsm_activities) +
      SUM(student_unique_article_header_views) AS nsm_actions
FROM  final_base_actions AS fba
JOIN final_engaged_article AS fea
ON fba.action_date=fea.action_date
JOIN dim.calendar cal
      ON fba.action_date=cal.date
GROUP BY
       fba.action_date,
       cal.date_key")

library(prophet)

mydata %>% filter(ACTION_DATE=='2022-07-02')

head(mydata %>% arrange(ACTION_DATE))
datafinal <- mydata %>%
  select(-DATE_KEY ) %>%
  rename(ds=ACTION_DATE,
         y=NSM_ACTIONS)
head(datafinal %>% arrange(ds))

#datafinal$floor=0
m <- prophet(datafinal,
             yearly.seasonality= 'auto',
             weekly.seasonality = 'auto',
             daily.seasonality= FALSE,
             seasonality.mode= 'multiplicative',
             seasonality.prior.scale= 0.03
 )


future <- make_future_dataframe(m, periods = 180)

forecast <- predict(m, future)
tail(forecast)
plot(m, forecast)


predict <- forecast %>%
  filter(ds >= '2023-01-31' & ds < '2023-06-30') %>%
  arrange(ds) %>%
  select(ds, yhat, yhat_upper)

prophet_plot_components(m, forecast)


df1 <- predict %>%
     mutate( yhat=if_else(yhat>0, yhat, 0) ) %>%
  arrange(yhat)


df2 <- mydata %>%
  filter(ACTION_DATE > '2022-06-30') %>%
  select(-DATE_KEY ) %>%
  rename(ds=ACTION_DATE,
         yhat=NSM_ACTIONS) %>%
  mutate(yhat_upper=0)

df1 %>% arrange(yhat)

union(df1, df2) %>%
  summarize(summation=sum(yhat) )
