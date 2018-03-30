!function () {
  /* 倍率设置. */
  const initScala = 0.1 // 初始缩放倍数.
  const stepScala = 0.1 // 每次放大时,相对于原尺寸的步进倍率.

  /* 原始图片宽高 */
  const oriImgWidth = 1920
  const oriImgHeight = 1080

  /* 基于准备好的dom，初始化echarts实例 */

  /* 更改以下数据,然后调用下 update 方法即可. */
  let currentScala = initScala // 当前显示倍率.

  /* 初始缩放倍率,适应屏幕宽高. */
  const winWidth = window.innerWidth || window.document.documentElement.clientWidth || window.document.body.clientWidth
  const winHeight = window.innerHeight || window.document.documentElement.clientHeight || window.document.body.clientHeight

  let widthScala = winWidth / oriImgWidth
  let heightScala = winHeight / oriImgHeight

  /* 把数组处理为键值对 城市名:[城市坐标] .延迟计算,会在第一次加载时,再计算.*/
  let transDataDict = null

  /* 改用固定宽高.
  如果是自动适配,可以使用代码
  currentScala = widthScala < heightScala ? widthScala : heightScala
   */
  currentScala = 1.0

  /* 车站和人流数据. */
  let data = stationGz;
  let links = []

  /* 处于激活状态的图标. */
  let currentChart = null;
  let currentChartDom = null

  /* 记录所有新站点数据. */
  let allLinks = []


  /* 数据更新周期. 秒 */
  const UPDATE_INTERVAL = 1.5

  /* 更新地铁数据. */
  function updateViewData()
  {
    /* 实现对象深拷贝. */
    const allLinksCopy = JSON.parse(JSON.stringify(allLinks))
    allLinks = []

    /* 要先数据去重. */
    let allLinksDict = {}
    let allLinksData = allLinksCopy.reduce((sumArr,linkItem) => {
      /* 实现对象深拷贝. */
      const newItem = JSON.parse(JSON.stringify(linkItem))

      const name = newItem.source
      const toname = newItem.target

      /* 使用起始站点名,来唯一标记数据. */
      const nameId = name + '-' + toname

      if ( ! allLinksDict[nameId]) {
        allLinksDict[nameId] = true
        sumArr.push(newItem)
      }

      return sumArr
    },[])
    updateView(currentChart,currentScala, data, allLinksData)
  }

  /* 更新人流信息的接口.只是实时存,但是更新,是周期性的. */
  function updateLinks(newLinks)
  {
    allLinks = allLinks.concat(newLinks)
  }

  /* 模拟socket调用,在部署时,此函数,请注释掉. */
  function startSocketMoco(callback){
    // /* 每100毫秒回调一次数据. */
    // let i = 0
    //
    // let rtn1 = [{"source":"团一大广场",value:1,"target":"西朗"}]
    // let rtn2 = [{"source":"北京路",value:1,"target":"中大"}]
    //
    //
    // setInterval(()=>{
    //   ++ i
    //   if (i < 3) {
    //     callback(rtn1)
    //   }else{
    //     callback(rtn2)
    //   }
    // }, 1000)

    /* 换成真实的函数调用. */
    Sub.subscribe({
      topic: "ticket:record:4401",
      pattern: false,
      handle: (data) => {
        /* 数据转换. */
        callback(convertData(data))
      }
    })
  }

  /* TODO: 把这一句注释掉,然后在真是的socket请求里,调用:
  updateLinks(data)
   */
  startSocketMoco(updateLinks)

  /* 初始显示. */
  function initShow() {
    /* 当前车站数据和人流信息,要动态从服务器获取. */
    /* 当前车站数据. */
  //     /* 车站人流信息. */
      links = []

      currentChartDom = document.createElement('div')
      currentChartDom.style.left = '0px'
      currentChartDom.style.top = '0px'
      currentChartDom.style.position = 'absolute'
      currentChartDom.style.width = oriImgWidth*currentScala + 'px'
      currentChartDom.style.height = oriImgHeight*currentScala + 'px'
      let parentDom = document.getElementById('subway')
      parentDom.appendChild(currentChartDom)
      currentChart = echarts.init(currentChartDom);
      setInterval(updateViewData, UPDATE_INTERVAL * 1000)
  }

  initShow()

  /* 根据数据或缩放情况,变化视图. */
  function updateView(myChart,currentScala, data, links) {
      /* 图片宽高. */
      const imgWidth = oriImgWidth * currentScala
      const imgHeight = oriImgHeight * currentScala

      /* 数据处理. */
      /* 站点数据,坐标变换. */
      let transData = data.map((item) => {
          /* 实现对象深拷贝. */
          const newItem = JSON.parse(JSON.stringify(item))

          newItem.value[0] = newItem.value[0] * currentScala
      newItem.value[1] = newItem.value[1] * currentScala
      newItem.value[1] = oriImgHeight * currentScala - newItem.value[1]

      return newItem
  }
  )

  /* 把数组处理为键值对 城市名:[城市坐标] */
  if ( ! transDataDict) {
    transDataDict = transData.reduce((sum, item) => {
            /* 实现对象深拷贝. */
            const newItem = JSON.parse(JSON.stringify(item))

            sum[newItem.name] = newItem.value

            return sum
        }, {}
    )
  }

  let transLinks = links.map((linkItem) => {
      /* 实现对象深拷贝. */
      const newItem = JSON.parse(JSON.stringify(linkItem))

      newItem.name = newItem.source
  newItem.toname = newItem.target
  newItem.coords = [transDataDict[newItem.name], transDataDict[newItem.toname]]
  return newItem
  })

  /* 需要加光圈的车站. */
  let topStationsDict = {}
  let topStationsData = links.reduce((sumArr, linkItem) => {
          /* 实现对象深拷贝. */
          const newItem = JSON.parse(JSON.stringify(linkItem))

          const name = newItem.source
          const toname = newItem.target

          if ( !topStationsDict[name])
  {
    topStationsDict[name] = true
    sumArr.push({
        name: name,
        value: transDataDict[name]
    })
  }

  if (!topStationsDict[toname]) {
    topStationsDict[toname] = true
    sumArr.push({
        name: toname,
        value: transDataDict[toname]
    })
  }

  return sumArr
  },
  []
  )

  /* 数据结构,应该联网获取. */
  let option = {
      grid: {
          left: 0,
          top: 0,
          right: 0,
          bottom: 0,
      },
      graphic: {
          type: 'image',
          style: {
              image: '',
              // image: './graph.jpg',
              width: imgWidth,
              height: imgHeight,
          },
          z: -1,
      },
      xAxis: {
          nameGap: 0,
          show: false,
          type: 'value',
          min: 0,
          max: imgWidth
      },
      yAxis: {
          nameGap: 0,
          show: false,
          type: 'value',
          min: 0,
          max: imgHeight,
      },

      series: [{
          zlevel: 1,
          type: 'lines',
          name: 'guangzhou-subway',
          coordinateSystem: 'cartesian2d',
          data: transLinks,
          //线上面的动态特效
          effect: {
              show: true,
              period: UPDATE_INTERVAL*0.9,
              trailLength: 0.01,
              //  飞线颜色
              //  color: '#6beedf',
              color: '#0de3cc',
              symbolSize: 5
          },
          lineStyle: {
              normal: {
                  width: '',
                  color: '#88dcda',
                  curveness: 0.2
              }
          },
          //  markPoint : {
          //      symbol:'circle',
          //      symbolSize : 30,
          //      label: {
          //        normal: {
          //          show: true,
          //          formatter: '{b}',
          //          textStyle: {
          //            color: '#f00'
          //          }
          //        }
          //      },
          //      effect : {
          //          show: false,
          //          shadowBlur : 0
          //      },
          //      data : [
          //          {name:'新城东', coord: [100, 100]},
          //          {name:'东圃',value:60},
          //          {name:'南村万博',value:30},
          //      ]
          //  }
      },
          {
              symbolSize: 10,
              symbol: "circle",
              "name": "Top 5",
              "type": "effectScatter",
              "coordinateSystem": "cartesian2d",

              //
              symbolSize: 13,

              "data": topStationsData,
              "showEffectOn": "render",
              "rippleEffect": {
                  "brushType": "stroke",
                  //换成另一种波纹
                  //
                  //  "brushType": "fill",
                  //  scale: 10,
                  scale: 3,

              },
              "hoverAnimation": true,
              "label": {
                  "normal": {
                      "formatter": "",
                      "position": "right",
                      "show": true
                  }
              },
              "itemStyle": {
                  "normal": {
                      //   圆点颜色  红色
                      //   "color": "#FF00FF",
                      //  #A8D4FF 蓝色
                      //    color: 'orange',
                      color: '#ffb400',
                      "shadowBlur": 10,
                      //   "shadowColor": "#f00",
                      "shadowColor": "#333",
                      opacity: 1
                  }
              },
              "zlevel": 2,
              //  markPoint : {
              //      symbol:'circle',
              //      symbolSize : 30,
              //      label: {
              //        normal: {
              //          show: true,
              //          formatter: '{b}',
              //          textStyle: {
              //            color: '#f00'
              //          }
              //        }
              //      },
              //      effect : {
              //          show: true,
              //          shadowBlur : 0
              //      },
              //      data : [
              //          {name:'新城东', coord: [100, 100]},
              //          {name:'东圃',value:60},
              //          {name:'南村万博',value:30},
              //      ]
              //  }
          }
      ]
  }

  // 使用刚指定的配置项和数据显示图表。
  myChart.setOption(option);
  }

  /* 数据转换. */
  function convertData(data) {
    if ( ! data || ! transDataDict) {
      return []
    }

    if (!data.entryStation || ! transDataDict[data.entryStation]) {
    //   console.error(data.TICKET_PICKUP_STATION_NAME_CN + '车站坐标数据不存在,请及时补充!')
      return []
    }

    if (!data.exitStation || ! transDataDict[data.exitStation]) {
    //   console.error(data.TICKET_GETOFF_STATION_NAME_CN + '车站坐标数据不存在,请及时补充!')
      return []
    }

    return [
      {
        source: data.entryStation,
        target: data.exitStation,
        value: data.ticketNum
      }
    ]
  }
}()
