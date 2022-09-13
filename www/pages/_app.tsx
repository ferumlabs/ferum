import "../styles/globals.css";
import DEFAULT_APP_SEO from "../next-seo.config";
import { DefaultSeo } from "next-seo";
import type { AppProps } from "next/app";
import Head from "next/head";
import PlausibleProvider from 'next-plausible'

function MyApp({ Component, pageProps }: AppProps) {
  return (
    <>
      <DefaultSeo {...DEFAULT_APP_SEO} />
      <Head>
        <meta
          name="viewport"
          content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=0, viewport-fit=cover"
        />
      </Head>
      <PlausibleProvider domain="ferum.xyz">
        <Component {...pageProps} />
      </PlausibleProvider>
    </>
  );
}

export default MyApp;
