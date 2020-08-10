# weblog_analysis
网站流量日志分析项目的数据预处理

## preprocess 模块
对网站采集日志数据文件进行预处理
## 点击流 pageviews 模型
Pageviews模型数据专注于用户每次会话（session）的识别，以及每次session内访问了几步和每一步的停留时间。
在网站分析中，通常把前后两条访问记录时间差在30分钟以内算成一次会话。如果超过30分钟，则把下次访问算成新的会话开始。
将清洗之后的日志梳理出点击流pageviews模型数据
区分出每一次会话，给每一次visit（session）增加了session-id（随机uuid）
## 点击流 visits 模型
Visit 模型专注于每次会话session内起始、结束的访问情况信息。
比如用户在某一个会话session内，进入会话的起始页面和起始时间，
会话结束是从哪个页面离开的，离开时间，本次session总共访问了几个页面等信息。
