import { NotificationsApi, NotificationsManager } from 'argo-ui';
import * as React from 'react';

export interface NotificationsContext {
    notifications: NotificationsApi;
}

export const notificationsManager = new NotificationsManager();
export const NotificationsContext = React.createContext<NotificationsContext>({
    notifications: notificationsManager,
});
