(function() {
  var style = getComputedStyle(document.documentElement);
  var accent = style.getPropertyValue('--accent').trim();
  var accent2 = style.getPropertyValue('--accent2').trim();
  var ink = style.getPropertyValue('--ink').trim();
  var muted = style.getPropertyValue('--muted').trim();
  var rule = style.getPropertyValue('--rule').trim();
  var bg2 = style.getPropertyValue('--bg2').trim();

  // Chart 1: MySQL status distribution
  var chart1 = echarts.init(document.getElementById('chart-status-dist'), null, { renderer: 'svg' });
  chart1.setOption({
    title: { text: '', left: 'center', textStyle: { fontSize: 14, color: ink } },
    tooltip: { trigger: 'item', appendToBody: true },
    legend: { bottom: 0, textStyle: { color: muted, fontSize: 12 } },
    series: [{
      type: 'pie',
      radius: ['40%', '70%'],
      center: ['50%', '45%'],
      label: { color: ink, fontSize: 12 },
      data: [
        { value: 2111, name: '200 OK', itemStyle: { color: accent } },
        { value: 85, name: '404 Not Found', itemStyle: { color: accent2 } },
        { value: 4, name: '500 Server Error', itemStyle: { color: '#e8483f' } }
      ]
    }],
    animation: false
  });
  window.addEventListener('resize', function() { chart1.resize(); });

  // Chart 2: Per-server traffic
  var chart2 = echarts.init(document.getElementById('chart-server-traffic'), null, { renderer: 'svg' });
  chart2.setOption({
    tooltip: { trigger: 'axis', appendToBody: true },
    legend: { bottom: 0, textStyle: { color: muted, fontSize: 12 } },
    grid: { left: '8%', right: '5%', top: '10%', bottom: '15%' },
    xAxis: { type: 'category', data: ['web-1', 'web-2', 'web-3'], axisLabel: { color: muted }, axisLine: { lineStyle: { color: rule } } },
    yAxis: { type: 'value', name: '', axisLabel: { color: muted }, splitLine: { lineStyle: { color: rule } } },
    series: [
      { name: 'Normal (200)', type: 'bar', data: [703, 710, 698], itemStyle: { color: accent }, barWidth: '30%' },
      { name: 'Errors (4xx/5xx)', type: 'bar', data: [35, 27, 27], itemStyle: { color: accent2 }, barWidth: '30%' }
    ],
    animation: false
  });
  window.addEventListener('resize', function() { chart2.resize(); });

  // Chart 3: Kafka Consumer Lag by partition
  var chart3 = echarts.init(document.getElementById('chart-kafka-lag'), null, { renderer: 'svg' });
  chart3.setOption({
    tooltip: { trigger: 'axis', appendToBody: true },
    grid: { left: '8%', right: '5%', top: '10%', bottom: '12%' },
    xAxis: { type: 'category', data: ['P0', 'P1', 'P2', 'P3', 'P4', 'P5'], axisLabel: { color: muted }, axisLine: { lineStyle: { color: rule } } },
    yAxis: { type: 'value', name: 'Lag', axisLabel: { color: muted }, splitLine: { lineStyle: { color: rule } } },
    series: [{
      name: 'Consumer Lag',
      type: 'bar',
      data: [9, 8, 9, 9, 0, 9],
      itemStyle: { color: accent2 },
      barWidth: '40%',
      label: { show: true, position: 'top', color: ink }
    }],
    animation: false
  });
  window.addEventListener('resize', function() { chart3.resize(); });

  // Chart 4: Container Resource Usage
  var chart4 = echarts.init(document.getElementById('chart-container-res'), null, { renderer: 'svg' });
  chart4.setOption({
    tooltip: { trigger: 'axis', appendToBody: true, axisPointer: { type: 'shadow' } },
    legend: { bottom: 0, textStyle: { color: muted, fontSize: 12 } },
    grid: { left: '8%', right: '5%', top: '10%', bottom: '15%' },
    xAxis: {
      type: 'category',
      data: ['Kafka-1', 'Kafka-2', 'Kafka-3', 'MySQL', 'Prometheus', 'Grafana', 'Consumer', 'cAdvisor'],
      axisLabel: { color: muted, fontSize: 11 },
      axisLine: { lineStyle: { color: rule } }
    },
    yAxis: [
      { type: 'value', name: 'CPU %', axisLabel: { color: muted }, splitLine: { lineStyle: { color: rule } } },
      { type: 'value', name: 'Memory (MiB)', axisLabel: { color: muted }, splitLine: { show: false } }
    ],
    series: [
      { name: 'CPU %', type: 'bar', data: [5.98, 5.19, 6.35, 7.60, 3.58, 3.75, 4.69, 11.28], itemStyle: { color: accent }, barWidth: '25%' },
      { name: 'Memory (MiB)', type: 'bar', yAxisIndex: 1, data: [512, 428, 481, 255, 218, 447, 29, 78], itemStyle: { color: accent2 }, barWidth: '25%' }
    ],
    animation: false
  });
  window.addEventListener('resize', function() { chart4.resize(); });

  // Chart 5: Alert response time
  var chart5 = echarts.init(document.getElementById('chart-alert-time'), null, { renderer: 'svg' });
  chart5.setOption({
    tooltip: { trigger: 'axis', appendToBody: true },
    grid: { left: '10%', right: '5%', top: '10%', bottom: '12%' },
    xAxis: { type: 'category', data: ['Min', 'Avg', 'Max'], axisLabel: { color: muted }, axisLine: { lineStyle: { color: rule } } },
    yAxis: { type: 'value', name: 'Time (s)', axisLabel: { color: muted }, splitLine: { lineStyle: { color: rule } } },
    series: [{
      name: 'Alert Latency',
      type: 'line',
      data: [0.336, 0.385, 0.583],
      itemStyle: { color: accent },
      lineStyle: { width: 3 },
      symbolSize: 10,
      label: { show: true, position: 'top', color: ink, formatter: '{c}s' }
    }],
    animation: false
  });
  window.addEventListener('resize', function() { chart5.resize(); });
})();
