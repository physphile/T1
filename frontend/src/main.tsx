import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import { App } from './App.tsx';
import '@gravity-ui/uikit/styles/fonts.css';
import '@gravity-ui/uikit/styles/styles.css';
import { ThemeProvider } from '@gravity-ui/uikit';

createRoot(document.getElementById('root')!).render(
	<StrictMode>
		<ThemeProvider theme="light">
			<App />
		</ThemeProvider>
	</StrictMode>
);
