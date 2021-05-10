package com.liubin.example

case class UserVisitData(
                          date: String,
                          userId: String,
                          sessionId: String,
                          pageId: Long,
                          actionTime: String,
                          searchKeyWord: String,
                          clickCategoryId: Long,
                          clickProductId: Long,
                          orderCategoryIds: String,
                          orderProductIds: String,
                          payCategoryIds: String,
                          payProductIds: String,
                          cityId: Long
                        )
