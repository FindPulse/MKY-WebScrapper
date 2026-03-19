/*************************************************************************
* ADOBE CONFIDENTIAL
* ___________________
*
*  Copyright 2015 Adobe Systems Incorporated
*  All Rights Reserved.
*
* NOTICE:  All information contained herein is, and remains
* the property of Adobe Systems Incorporated and its suppliers,
* if any.  The intellectual and technical concepts contained
* herein are proprietary to Adobe Systems Incorporated and its
* suppliers and are protected by all applicable intellectual property laws,
* including trade secret and or copyright laws.
* Dissemination of this information or reproduction of this material
* is strictly forbidden unless prior written permission is obtained
* from Adobe Systems Incorporated.
**************************************************************************/
import{loggingApi as e}from"../common/loggingApi.js";import{dcSessionStorage as o}from"../common/local-storage.js";import{DIRECT_FLOW as r}from"./constant.js";import{analytics as t}from"../common/analytics.js";export function executeDirectVerb(o){try{let e=o.srcUrl;e=`${e}&acrobatPromotionSource=${o.promotionSource}`;const r=o.name,t=o.verb,c=`${o.viewerURL}?pdfurl=${encodeURIComponent(e)}&acrobatPromotionWorkflow=${t}&pdffilename=${encodeURIComponent(r)}`;chrome.tabs.create({url:c,active:!0})}catch(o){e.error({message:"Error executing direct verb",error:o.message})}}export function removeAllDirectFlowSessionsFromLoading(e){const t=[];let c=o.getItem(r.SESSIONS_WHERE_VIEWER_LOADING)||[];return c=c.filter(o=>o.tabId!==e||(t.push(o),!1)),t.length>0&&o.setItem(r.SESSIONS_WHERE_VIEWER_LOADING,c),t}export function markDirectFlowSuccess(e){const t=(o.getItem(r.SESSIONS_WHERE_VIEWER_LOADING)||[]).map(o=>o.tabId===e?{...o,directFlowSuccess:!0}:o);o.setItem(r.SESSIONS_WHERE_VIEWER_LOADING,t)}export function directFlowTabCloseListener(e){removeAllDirectFlowSessionsFromLoading(e).forEach(e=>{const o=e.startTime?Date.now()-e.startTime:void 0,r=e.directFlowSuccess?"afterDirectFlowSuccess":"beforeDirectFlowSuccess";t.event("DCBrowserExt:Viewer:DirectFlow:TabClosedBeforeViewerLoaded",{source:e.source,loadTime:o,eventContext:r})})}export function directFlowTabNavigatedAwayListener(e,o,r){r&&r(o)||removeAllDirectFlowSessionsFromLoading(e)}