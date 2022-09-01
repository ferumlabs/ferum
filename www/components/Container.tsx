import {ReactNode} from 'react';

import Navigation from "./Navigation";


type ContainerProps = {
  showsNavigation?: boolean,
  disableResponsive?: boolean 
  disableMargins?: boolean,
  children: ReactNode[] | ReactNode
}

const Container = ({showsNavigation, disableResponsive, disableMargins, children}: ContainerProps) => {
  return (
    // 1. There is a top level flex box with height 100%.
    // 2. The first child is an optional navigation bar with a set height and no shrink.
    // 3. The second child is a div with responsive horizontal margins set to grow fully.
    // 4. The second child's top is offset by navigation's height to allow centering on screen.
    // 5. Z index on the child that has navigation is increased to prevent the second child eating events.
    <div className="flex flex-col h-full">
      {showsNavigation && (
        <div className="shrink-0 z-50">
          <Navigation />
        </div>
      )}
      <div
        className={`grow ${
          disableResponsive 
            ? "w-full" 
            : "xl:max-w-7xl lg:mx-auto md:max-w-5xl"
        } ${
          !disableMargins
          && "mx-8"
        } ${
          showsNavigation && "-mt-34"
        }`}
      >
        {children}
      </div>
    </div>
  );
};

Container.defaultProps = {};

export default Container;
